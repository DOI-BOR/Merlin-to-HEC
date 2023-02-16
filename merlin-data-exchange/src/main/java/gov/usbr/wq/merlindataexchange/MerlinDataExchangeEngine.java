package gov.usbr.wq.merlindataexchange;

import gov.usbr.wq.dataaccess.MerlinTimeSeriesDataAccess;
import gov.usbr.wq.dataaccess.http.ApiConnectionInfo;
import gov.usbr.wq.dataaccess.http.HttpAccessException;
import gov.usbr.wq.dataaccess.http.HttpAccessUtils;
import gov.usbr.wq.dataaccess.http.TokenContainer;
import gov.usbr.wq.dataaccess.model.MeasureWrapper;
import gov.usbr.wq.dataaccess.model.QualityVersionWrapper;
import gov.usbr.wq.dataaccess.model.TemplateWrapper;
import gov.usbr.wq.merlindataexchange.configuration.DataExchangeConfiguration;
import gov.usbr.wq.merlindataexchange.configuration.DataExchangeSet;
import gov.usbr.wq.merlindataexchange.configuration.DataStore;
import gov.usbr.wq.merlindataexchange.configuration.DataStoreRef;
import gov.usbr.wq.merlindataexchange.io.DataExchangeDao;
import gov.usbr.wq.merlindataexchange.io.DataExchangeDaoFactory;
import hec.ui.ProgressListener;
import hec.ui.ProgressListener.MessageType;
import rma.services.annotations.ServiceProvider;

import javax.xml.stream.XMLStreamException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.FileHandler;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

import static java.util.stream.Collectors.toList;

public final class MerlinDataExchangeEngine implements DataExchangeEngine
{
    private static final Logger LOG_FILE_LOGGER = Logger.getLogger("MerlinDataExchange");
    private static final Logger LOGGER = Logger.getLogger(MerlinDataExchangeEngine.class.getName());
    private static final int PERCENT_COMPLETE_ALLOCATED_FOR_INITIAL_SETUP = 5;
    private static final int THREAD_COUNT = 5;
    private static final String THREAD_PROPERTY_KEY = "merlin.dataexchange.threadpool.size";
    private final ExecutorService _executorService = Executors.newFixedThreadPool(getThreadPoolSize(), new MerlinThreadFactory());
    private final Map<String, DataExchangeCache> _dataExchangeCache = new HashMap<>();
    private final MerlinTimeSeriesDataAccess _merlinDataAccess = new MerlinTimeSeriesDataAccess();
    private final AtomicBoolean _isCancelled = new AtomicBoolean(false);
    private ProgressListener _progressListener;
    private MerlinExchangeDaoCompletionTracker _completionTracker;
    private final Map<String, Handler> _fileHandlers = new HashMap<>();

    private static int getThreadPoolSize()
    {
        int retVal;
        //availableProcessors gives us logical cores, which includes hyperthreading stuff.  We can't determine if hyperthreading is on, so let's always halve the available processors.
        //Let's make sure we don't go lower than 1 by using Math.max.  1 / 2 = 0 in integer values, so this could be bad...
        String threadPoolSize = System.getProperty(THREAD_PROPERTY_KEY);
        if (threadPoolSize != null)
        {
            retVal = Math.max(Integer.parseInt(threadPoolSize), 1);
            LOGGER.log(Level.FINE, () -> "Merlin executor service created using System Property " + THREAD_PROPERTY_KEY + " with thread pool size of: " + retVal);
        }
        else
        {
            int coreCount = Math.max(getCoreCount(), 1);
            retVal = THREAD_COUNT * coreCount; //5 should cover bases for concurrent merlin web data retrieval?
            LOGGER.log(Level.FINE, () -> "System Property " + THREAD_PROPERTY_KEY + " not set. Merlin executor service created using default thread pool size of: " + retVal);
        }
        return retVal;
    }

    private static int getCoreCount()
    {
        return Runtime.getRuntime().availableProcessors() / 2;
    }

    @Override
    public CompletableFuture<MerlinDataExchangeStatus> runExtract(List<Path> xmlConfigurationFiles, MerlinDataExchangeParameters runtimeParameters, ProgressListener progressListener)
    {
        _progressListener = Objects.requireNonNull(progressListener, "Missing required ProgressListener");
        _isCancelled.set(false);
        _progressListener.start();
        Map<Path, DataExchangeConfiguration> parsedConfigurations = parseConfigurations(xmlConfigurationFiles);
        return beginExtract(parsedConfigurations, runtimeParameters);
    }

    @Override
    public void cancelExtract()
    {
        _isCancelled.set(true);
        if(_progressListener != null)
        {
            logProgress("Merlin Data Exchanged Cancelled. Finishing gracefully...");
        }
    }

    private CompletableFuture<MerlinDataExchangeStatus> beginExtract(Map<Path, DataExchangeConfiguration> parsedConfiguartions,
                                                  MerlinDataExchangeParameters runtimeParameters)
    {
        return CompletableFuture.supplyAsync(() ->
        {
            MerlinDataExchangeStatus retVal = MerlinDataExchangeStatus.FAILURE;
            try
            {
                setUpLoggingForConfigs(parsedConfiguartions, runtimeParameters.getLogFileDirectory());
                List<ApiConnectionInfo> merlinRoots = getMerlinUrlPaths(parsedConfiguartions.values());
                for(ApiConnectionInfo connectionInfo : merlinRoots)
                {
                    TokenContainer token = HttpAccessUtils.authenticate(connectionInfo, runtimeParameters.getUsername(), runtimeParameters.getPassword());
                    initializeCachePerUrl(parsedConfiguartions, runtimeParameters.getLogFileDirectory(), connectionInfo, token);
                }
                //here we could also initialize a cache with a dss path if we reverse the direction in future to read from DSS and write to Merlin
                if(!_isCancelled.get())
                {
                    extractUsingInitializedCache(parsedConfiguartions, runtimeParameters);
                }
                finish();
                retVal = _completionTracker.getCompletionStatus();
            }
            catch (IOException e)
            {
                logError("Failed to initialize templates, quality versions, and measures cache", e);
            }
            catch (HttpAccessException e)
            {
                logError("Failed to authenticate user: " + runtimeParameters.getUsername(), e);
            }
            return retVal;
        }, _executorService);
    }

    private List<ApiConnectionInfo> getMerlinUrlPaths(Collection<DataExchangeConfiguration> configs)
    {
        List<String> retVal = new ArrayList<>();
        for(DataExchangeConfiguration config : configs)
        {
            List<DataExchangeSet> sets = config.getDataExchangeSets();
            for(DataExchangeSet set : sets)
            {
                appendPathsFromSetForDataType(config, set, "merlin", retVal);
            }
        }
        return retVal.stream()
                .map(ApiConnectionInfo::new)
                .collect(toList());
    }

    private void appendPathsFromSetForDataType(DataExchangeConfiguration config, DataExchangeSet set, String type, List<String> retVal)
    {
        Optional<DataStore> dataStoreA = config.getDataStoreByRef(set.getDataStoreRefA());
        Optional<DataStore> dataStoreB = config.getDataStoreByRef(set.getDataStoreRefB());
        if(dataStoreA.isPresent() && type.equalsIgnoreCase(dataStoreA.get().getDataStoreType()))
        {
            String url = dataStoreA.get().getPath();
            if(!retVal.contains(url))
            {
                retVal.add(url);
            }
        }
        if(dataStoreB.isPresent() && type.equalsIgnoreCase(dataStoreB.get().getDataStoreType()))
        {
            String url = dataStoreB.get().getPath();
            if(!retVal.contains(url))
            {
                retVal.add(url);
            }
        }
    }

    private void extractUsingInitializedCache(Map<Path, DataExchangeConfiguration> parsedConfiguartions, MerlinDataExchangeParameters runtimeParameters)
    {
        int totalSeriesIds = _dataExchangeCache.values().stream()
                .map(dxCache -> getTotalNumberOfSeries(dxCache.getCachedTemplateToSeriesIds()))
                .collect(toList()).stream()
                .mapToInt(Integer::intValue)
                .sum();
        _completionTracker = new MerlinExchangeDaoCompletionTracker(totalSeriesIds, PERCENT_COMPLETE_ALLOCATED_FOR_INITIAL_SETUP);
        for(Map.Entry<Path, DataExchangeConfiguration> entry : parsedConfiguartions.entrySet())
        {
            Path configPath = entry.getKey();
            DataExchangeConfiguration dataExchangeConfiguration = entry.getValue();
            setLogHandlerForConfig(configPath, runtimeParameters.getLogFileDirectory());
            extractConfiguration(dataExchangeConfiguration, runtimeParameters);
            String errorMessage = "Finished extract for " + configPath;
            LOG_FILE_LOGGER.info(errorMessage);
            _progressListener.progress(errorMessage, MessageType.IMPORTANT);
        }
    }

    private int getTotalNumberOfSeries(Map<TemplateWrapper, List<String>> measuresMap)
    {
        return measuresMap.values()
                .stream()
                .mapToInt(List::size)
                .sum();
    }

    private void finish()
    {
        try
        {
            if (_isCancelled.get())
            {
                logProgress("Merlin Data Exchanged Cancelled successfully");
            }
            else
            {
                logProgress("Merlin Data Exchange Complete");
            }
            _progressListener.finish();
        }
        finally
        {
            for(Handler h: _fileHandlers.values())
            {
                h.close();
            }
            _fileHandlers.clear();
            _dataExchangeCache.clear();
        }
    }

    private Map<Path, DataExchangeConfiguration> parseConfigurations(List<Path> xmlConfigurationFiles)
    {
        Map<Path, DataExchangeConfiguration> retVal = new TreeMap<>();
        xmlConfigurationFiles.forEach(configFilepath -> retVal.put(configFilepath, parseDataExchangeConfiguration(configFilepath)));
        logProgress("Configuration files parsed", 1);
        return retVal;
    }

    private void extractConfiguration(DataExchangeConfiguration dataExchangeConfig, MerlinDataExchangeParameters runtimeParameters)
    {
        if (dataExchangeConfig != null)
        {
            List<DataExchangeSet> dataExchangeSets = dataExchangeConfig.getDataExchangeSets();
            dataExchangeSets.forEach(dataExchangeSet ->
            {
                if(!_isCancelled.get())
                {
                    exchangeDataForSet(dataExchangeSet, dataExchangeConfig, runtimeParameters);
                }
            });
        }
    }

    private void setLogHandlerForConfig(Path configPath, Path logDirectory)
    {
        String configNameWithoutExtension = configPath.getFileName().toString().split("\\.")[0];
        Path logFile = logDirectory.resolve(configNameWithoutExtension + ".log");
        LOG_FILE_LOGGER.setUseParentHandlers(false);
        if (LOG_FILE_LOGGER.getHandlers().length > 0)
        {
            for(int i = LOG_FILE_LOGGER.getHandlers().length -1; i >= 0; i--)
            {
                LOG_FILE_LOGGER.removeHandler(LOG_FILE_LOGGER.getHandlers()[i]);
            }
        }
        Handler fileHandler = _fileHandlers.get(logFile.toString());
        LOG_FILE_LOGGER.addHandler(fileHandler);
        SimpleFormatter formatter = new SimpleFormatter();
        fileHandler.setFormatter(formatter);
    }

    private void setUpLoggingForConfigs(Map<Path, DataExchangeConfiguration> parsedConfiguartions, Path logDirectory)
    {
        List<String> configNameWithoutExtensions = parsedConfiguartions.keySet().stream().map(c -> c.getFileName().toString().split("\\.")[0])
                .collect(toList());
        for(String configNameWithoutExtension : configNameWithoutExtensions)
        {
            Path logFile = logDirectory.resolve(configNameWithoutExtension + ".log");
            try
            {
                Files.createDirectories(logDirectory);
                new FileOutputStream(logFile.toString(), false).close();
                FileHandler fileHandler = new FileHandler(logFile.toString());
                _fileHandlers.put(logFile.toString(), fileHandler);
                LOG_FILE_LOGGER.setUseParentHandlers(false);
                LOG_FILE_LOGGER.addHandler(fileHandler);
                SimpleFormatter formatter = new SimpleFormatter();
                fileHandler.setFormatter(formatter);
            }
            catch (IOException e)
            {
                LOGGER.log(Level.SEVERE, e, () -> "Failed to create log file: " + logFile);
            }
        }
    }

    private void exchangeDataForSet(DataExchangeSet dataExchangeSet, DataExchangeConfiguration dataExchangeConfig,
                                    MerlinDataExchangeParameters runtimeParameters)
    {
        DataStoreRef dataStoreRefB = dataExchangeSet.getDataStoreRefB();
        DataStoreRef dataStoreRefA = dataExchangeSet.getDataStoreRefA();
        DataStoreRef sourceRef = dataStoreRefA;
        DataStoreRef destinationRef = dataStoreRefB;
        if (dataExchangeSet.getSourceId().equalsIgnoreCase(dataStoreRefB.getId()))
        {
            destinationRef = dataStoreRefA;
            sourceRef = dataStoreRefB;
        }
        Optional<DataStore> dataStoreDestinationOpt = dataExchangeConfig.getDataStoreByRef(destinationRef);
        Optional<DataStore> dataStoreSourceOpt = dataExchangeConfig.getDataStoreByRef(sourceRef);
        if(!dataStoreDestinationOpt.isPresent())
        {
            String destinationRefIdId = destinationRef.getId();
            logError("No datastore found for datastore-ref id: " + destinationRefIdId, null);
        }
        if(!dataStoreSourceOpt.isPresent())
        {
            String destinationRefIdId = destinationRef.getId();
            logError("No datastore found for datastore-ref id: " + destinationRefIdId, null);
        }
        if(dataStoreDestinationOpt.isPresent() && dataStoreSourceOpt.isPresent())
        {
            exchangeData(dataExchangeConfig, dataExchangeSet, dataStoreSourceOpt.get(), dataStoreDestinationOpt.get(), runtimeParameters);
        }
    }

    private String getDataStoreSourcePath(DataExchangeConfiguration dataExchangeConfig, DataExchangeSet dataExchangeSet)
    {
        DataStoreRef dataStoreRefA = dataExchangeSet.getDataStoreRefA();
        DataStoreRef dataStoreRefB = dataExchangeSet.getDataStoreRefB();
        DataStoreRef sourceRef = dataStoreRefA;
        DataStoreRef destinationRef = dataStoreRefB;
        if (dataExchangeSet.getSourceId().equalsIgnoreCase(dataStoreRefB.getId()))
        {
            destinationRef = dataStoreRefA;
            sourceRef = dataStoreRefB;
        }
        Optional<DataStore> dataStoreSourceOpt = dataExchangeConfig.getDataStoreByRef(sourceRef);
        String retVal = "";
        if(!dataStoreSourceOpt.isPresent())
        {
            String destinationRefIdId = destinationRef.getId();
            logError("No datastore found for datastore-ref id: " + destinationRefIdId, null);
        }
        else
        {
            retVal = dataStoreSourceOpt.get().getPath();
        }
        return retVal;
    }

    private void exchangeData(DataExchangeConfiguration config, DataExchangeSet dataExchangeSet, DataStore dataStoreSource, DataStore dataStoreDestination,
                              MerlinDataExchangeParameters runtimeParameters)
    {
        TemplateWrapper template = getTemplateFromDataExchangeSet(config, dataExchangeSet);
        if(template != null)
        {
            String unitSystemToConvertTo = dataExchangeSet.getUnitSystem();
            logProgress("Specified unit system: " + unitSystemToConvertTo);
            DataExchangeDao dao = DataExchangeDaoFactory.lookupDao(dataStoreSource, dataStoreDestination, LOG_FILE_LOGGER);
            String sourcePath = dataStoreSource.getPath(); //for merlin this is a URL
            DataExchangeCache cache = _dataExchangeCache.get(sourcePath);
            List<String> seriesIds = cache.getCachedTemplateToSeriesIds().get(template);
            List<CompletableFuture<Void>> measurementFutures = new ArrayList<>();
            try
            {
                seriesIds.forEach(seriesId ->
                        measurementFutures.add(dao.exchangeData(dataExchangeSet, runtimeParameters, cache,
                                dataStoreSource, dataStoreDestination, seriesId, _completionTracker, _progressListener,
                                _isCancelled, LOG_FILE_LOGGER, _executorService)));
                CompletableFuture.allOf(measurementFutures.toArray(new CompletableFuture[0])).join();
            }
            finally
            {
                dao.cleanUp();
            }

        }
    }

    private void logProgress(String message)
    {
        if(_progressListener != null)
        {
            _progressListener.progress(message, MessageType.IMPORTANT);
        }
        LOG_FILE_LOGGER.info(() -> message);
    }

    private void logProgress(String message, int progressPercentage)
    {
        if(_progressListener != null)
        {
            _progressListener.progress(message, MessageType.IMPORTANT, progressPercentage);
        }
        LOG_FILE_LOGGER.info(() -> message);
    }

    private void logError(String message, Throwable error)
    {
        if(_progressListener != null)
        {
            _progressListener.progress(message, MessageType.ERROR);
        }
        LOG_FILE_LOGGER.severe(() -> message);
        if(error != null)
        {
            LOGGER.log(Level.CONFIG, () -> message);
        }
        else
        {
            LOGGER.log(Level.CONFIG, error, () -> message);
        }
    }

    private List<MeasureWrapper> retrieveMeasures(ApiConnectionInfo connectionInfo, TokenContainer accessToken, TemplateWrapper template, ProgressListener progressListener)
    {
        List<MeasureWrapper> retVal = new ArrayList<>();
        try
        {
            logProgress("Retrieving measures for template " + template.getName() + " (id: " + template.getDprId() + ")...");
            retVal = _merlinDataAccess.getMeasurementsByTemplate(connectionInfo, accessToken, template);
            logProgress("Successfully retrieved " + retVal.size() + " measures!", PERCENT_COMPLETE_ALLOCATED_FOR_INITIAL_SETUP);
        }
        catch (HttpAccessException | IOException ex)
        {
            LOG_FILE_LOGGER.log(Level.SEVERE, ex, () -> "Unable to access the merlin web services to retrieve measures for template " + template);
            progressListener.progress("Failed to retrieve measures for template " + template.getName() + " (id: " + template.getDprId() + ")", MessageType.ERROR);
            LOGGER.log(Level.CONFIG, ex, () -> "Unable to access the merlin web services to retrieve measures for template " + template);
        }
        return retVal;
    }

    private TemplateWrapper getTemplateFromDataExchangeSet(DataExchangeConfiguration dataExchangeConfiguration, DataExchangeSet dataExchangeSet)
    {
        String sourcePath = getDataStoreSourcePath(dataExchangeConfiguration, dataExchangeSet);
        String templateNameFromSet = dataExchangeSet.getTemplateName();
        int templateIdFromSet = dataExchangeSet.getTemplateId();
        List<TemplateWrapper> cachedTemplates = _dataExchangeCache.get(sourcePath).getCachedTemplates();
        TemplateWrapper retVal = cachedTemplates.stream()
                .filter(template -> template.getName().equalsIgnoreCase(templateNameFromSet))
                .findFirst()
                .orElse(null);
        if(retVal == null)
        {
            retVal = cachedTemplates.stream()
                    .filter(template -> template.getDprId() == templateIdFromSet)
                    .findFirst()
                    .orElse(null);
        }
        if(retVal == null)
        {
            String errorMsg = "Failed to find matching template ID in retrieved templates for template name " + templateNameFromSet + " or id " + templateIdFromSet;
            logError(errorMsg, null);
        }
        return retVal;
    }

    private DataExchangeConfiguration parseDataExchangeConfiguration(Path xmlConfigurationFile)
    {
        DataExchangeConfiguration retVal = null;
        try
        {
            logProgress("Parsing configuration file: " + xmlConfigurationFile.toString() + "...");
            retVal = MerlinDataExchangeParser.parseXmlFile(xmlConfigurationFile);
            logProgress("Parsed configuration file successfully!");
        }
        catch (IOException | XMLStreamException e)
        {
            String errorMsg = "Failed to parse data exchange configuration xml for " + xmlConfigurationFile.toString();
            logError(errorMsg, e);
        }
        return retVal;
    }

    private void initializeCachePerUrl(Map<Path, DataExchangeConfiguration> parsedConfigurations, Path logFileDirectory, ApiConnectionInfo connectionInfo, TokenContainer token)
            throws IOException, HttpAccessException
    {

        DataExchangeCache cache = _dataExchangeCache.get(connectionInfo.getApiRoot());
        if(cache == null)
        {
            cache = new DataExchangeCache();
            _dataExchangeCache.put(connectionInfo.getApiRoot(), cache);
            logProgress("Retrieving templates from Merlin Web Service");
            List<TemplateWrapper> templates = _merlinDataAccess.getTemplates(connectionInfo, token);
            cache.cacheTemplates(templates);
            logProgress("Successfully retrieved " + templates.size() + " templates.", (int) (PERCENT_COMPLETE_ALLOCATED_FOR_INITIAL_SETUP * 0.4));
            logProgress("Retrieving quality versions from Merlin Web Service");
            List<QualityVersionWrapper> qualityVersions = _merlinDataAccess.getQualityVersions(connectionInfo, token);
            cache.cacheQualityVersions(qualityVersions);
            logProgress("Successfully retrieved " + qualityVersions.size() + " quality versions.", (int) (PERCENT_COMPLETE_ALLOCATED_FOR_INITIAL_SETUP * 0.6));
            if(!_isCancelled.get())
            {
                initializeCachedMeasurementsPerUrl(cache, parsedConfigurations, logFileDirectory, connectionInfo, token);
            }
        }
        else
        {
            initializeCachedMeasurementsPerUrl(cache, parsedConfigurations, logFileDirectory, connectionInfo, token);
        }
    }

    private void initializeCachedMeasurementsPerUrl(DataExchangeCache cache, Map<Path, DataExchangeConfiguration> parsedConfigurations, Path logFileDirectory,
                                                    ApiConnectionInfo connectionInfo, TokenContainer token)
    {
        for(Map.Entry<Path, DataExchangeConfiguration> entry : parsedConfigurations.entrySet())
        {
            Path configFile = entry.getKey();
            DataExchangeConfiguration dataExchangeConfig = entry.getValue();
            setLogHandlerForConfig(configFile, logFileDirectory);
            List<DataExchangeSet> exchangeSets = dataExchangeConfig.getDataExchangeSets();
            exchangeSets.forEach(set ->
            {
                TemplateWrapper template = getTemplateFromDataExchangeSet(dataExchangeConfig, set);
                if(template != null && !cache.getCachedTemplateToSeriesIds().containsKey(template))
                {
                    List<MeasureWrapper> measures = retrieveMeasures(connectionInfo, token, template, _progressListener);
                    List<String> seriesIds = measures.stream().map(MeasureWrapper::getSeriesString).collect(toList());
                    cache.cacheSeriesIds(template, seriesIds);
                }
            });
        }
    }

}
