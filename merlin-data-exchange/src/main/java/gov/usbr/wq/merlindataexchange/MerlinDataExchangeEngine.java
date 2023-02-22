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
import gov.usbr.wq.merlindataexchange.io.DataExchangeIO;
import gov.usbr.wq.merlindataexchange.io.DataExchangeLookupException;
import gov.usbr.wq.merlindataexchange.io.DataExchangeReader;
import gov.usbr.wq.merlindataexchange.io.DataExchangeReaderFactory;
import gov.usbr.wq.merlindataexchange.io.DataExchangeWriter;
import gov.usbr.wq.merlindataexchange.io.DataExchangeWriterFactory;
import gov.usbr.wq.merlindataexchange.io.MerlinDataExchangeReader;
import gov.usbr.wq.merlindataexchange.parameters.MerlinParameters;
import gov.usbr.wq.merlindataexchange.parameters.UsernamePasswordHolder;
import gov.usbr.wq.merlindataexchange.parameters.UsernamePasswordNotFoundException;
import hec.ui.ProgressListener;
import hec.ui.ProgressListener.MessageType;

import javax.xml.stream.XMLStreamException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
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
import java.util.regex.Pattern;

import static java.util.stream.Collectors.toList;

public final class MerlinDataExchangeEngine implements DataExchangeEngine
{
    private static final Logger LOGGER = Logger.getLogger(MerlinDataExchangeEngine.class.getName());
    private static final int PERCENT_COMPLETE_ALLOCATED_FOR_INITIAL_SETUP = 5;
    private static final int THREAD_COUNT = 5;
    private static final String THREAD_PROPERTY_KEY = "merlin.dataexchange.threadpool.size";
    private static final Pattern LOG_PATTERN = Pattern.compile(".log$");
    private final Logger _logFileLogger = Logger.getLogger(getClass().getName() + hashCode());
    private final ExecutorService _executorService = Executors.newFixedThreadPool(getThreadPoolSize(), new MerlinThreadFactory());
    private final Map<ApiConnectionInfo, DataExchangeCache> _dataExchangeCache = new HashMap<>();
    private final MerlinTimeSeriesDataAccess _merlinDataAccess = new MerlinTimeSeriesDataAccess();
    private final AtomicBoolean _isCancelled = new AtomicBoolean(false);
    private final List<Path> _configurationFiles;
    private final MerlinParameters _runtimeParameters;
    private final ProgressListener _progressListener;
    private final MerlinExchangeDaoCompletionTracker _completionTracker = new MerlinExchangeDaoCompletionTracker(PERCENT_COMPLETE_ALLOCATED_FOR_INITIAL_SETUP);
    private final Map<String, Handler> _fileHandlers = new HashMap<>();
    private CompletableFuture<MerlinDataExchangeStatus> _extractFuture;

    MerlinDataExchangeEngine(List<Path> configurationFiles, MerlinParameters runtimeParameters, ProgressListener progressListener)
    {
        _configurationFiles = configurationFiles;
        _runtimeParameters = runtimeParameters;
        _progressListener = progressListener;
    }


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
    public synchronized CompletableFuture<MerlinDataExchangeStatus> runExtract()
    {
        if(_extractFuture == null || _extractFuture.isDone())
        {
            _isCancelled.set(false);
            if(_progressListener != null)
            {
                _progressListener.start();
            }
            Map<Path, DataExchangeConfiguration> parsedConfigurations = parseConfigurations();
            _extractFuture = beginExtract(parsedConfigurations);
        }
        return _extractFuture;
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

    private CompletableFuture<MerlinDataExchangeStatus> beginExtract(Map<Path, DataExchangeConfiguration> parsedConfiguartions)
    {
        return CompletableFuture.supplyAsync(() ->
        {
            MerlinDataExchangeStatus retVal = MerlinDataExchangeStatus.FAILURE;
            try
            {
                setUpLoggingForConfigs(parsedConfiguartions, _runtimeParameters.getLogFileDirectory());
                logProgress("Setting up extract for all configs");
                List<ApiConnectionInfo> merlinRoots = getMerlinUrlPaths(parsedConfiguartions.values());
                for(ApiConnectionInfo connectionInfo : merlinRoots)
                {
                    initializeCacheForMerlinUrl(connectionInfo, parsedConfiguartions);
                }
                if(!_isCancelled.get())
                {
                    logProgress("Setup complete for all configs");
                    extractUsingInitializedCache(parsedConfiguartions);
                }
                finish();
                retVal = _completionTracker.getCompletionStatus();
            }
            catch (IOException e)
            {
                logError("Failed to initialize templates, quality versions, and measures cache", e);
            }
            return retVal;
        }, _executorService);
    }

    private void initializeCacheForMerlinUrl(ApiConnectionInfo connectionInfo, Map<Path, DataExchangeConfiguration> parsedConfiguartions) throws IOException
    {
        try
        {
            UsernamePasswordHolder usernamePassword = _runtimeParameters.getUsernamePasswordForUrl(connectionInfo.getApiRoot());
            initializeCacheForMerlinUrlWithAuthentication(connectionInfo, parsedConfiguartions, usernamePassword);
        }
        catch (UsernamePasswordNotFoundException e)
        {
            logError("Failed to match username/password for URL in config: " + connectionInfo.getApiRoot(), e);
            throw new IOException(e);
        }
    }

    private void initializeCacheForMerlinUrlWithAuthentication(ApiConnectionInfo connectionInfo, Map<Path, DataExchangeConfiguration> parsedConfiguartions, UsernamePasswordHolder usernamePassword)
            throws IOException
    {
        try
        {
            TokenContainer token = HttpAccessUtils.authenticate(connectionInfo, usernamePassword.getUsername(), usernamePassword.getPassword());
            initializeCacheForMerlinWithToken(parsedConfiguartions, _runtimeParameters.getLogFileDirectory(), connectionInfo, token);
        }
        catch (HttpAccessException e)
        {
            logError("Failed to authenticate user: " + usernamePassword.getUsername() + " for URL: " + connectionInfo.getApiRoot(), e);
            throw new IOException(e);
        }
    }

    private List<ApiConnectionInfo> getMerlinUrlPaths(Collection<DataExchangeConfiguration> configs)
    {
        List<String> retVal = new ArrayList<>();
        for(DataExchangeConfiguration config : configs)
        {
            List<DataExchangeSet> sets = config.getDataExchangeSets();
            for(DataExchangeSet set : sets)
            {
                appendPathsFromSetForDataType(config, set, MerlinDataExchangeReader.MERLIN, retVal);
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

    private void extractUsingInitializedCache(Map<Path, DataExchangeConfiguration> parsedConfiguartions)
    {
        for(Map.Entry<Path, DataExchangeConfiguration> entry : parsedConfiguartions.entrySet())
        {
            Path configPath = entry.getKey();
            DataExchangeConfiguration dataExchangeConfiguration = entry.getValue();
            specifyLogHandlerToUseForConfig(configPath, _runtimeParameters.getLogFileDirectory());
            extractConfiguration(dataExchangeConfiguration);
            String logMessage = "Finished extract for " + configPath;
            logProgress(logMessage);
        }
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
            if(_progressListener != null)
            {
                _progressListener.finish();
            }
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

    private Map<Path, DataExchangeConfiguration> parseConfigurations()
    {
        Map<Path, DataExchangeConfiguration> retVal = new TreeMap<>();
        _configurationFiles.forEach(configFilepath -> retVal.put(configFilepath, parseDataExchangeConfiguration(configFilepath)));
        logProgress("Configuration files parsed", 1);
        return retVal;
    }

    private void extractConfiguration(DataExchangeConfiguration dataExchangeConfig)
    {
        if (dataExchangeConfig != null)
        {
            List<DataExchangeSet> dataExchangeSets = dataExchangeConfig.getDataExchangeSets();
            dataExchangeSets.forEach(dataExchangeSet ->
            {
                if(!_isCancelled.get())
                {
                    exchangeDataForSet(dataExchangeSet, dataExchangeConfig);
                }
            });
        }
    }

    private void specifyLogHandlerToUseForConfig(Path configPath, Path logDirectory)
    {
        String configNameWithoutExtension = configPath.getFileName().toString().split("\\.")[0];
        Path logFile = logDirectory.resolve(configNameWithoutExtension + ".log");
        _logFileLogger.setUseParentHandlers(false);
        if (_logFileLogger.getHandlers().length > 0)
        {
            for(int i = _logFileLogger.getHandlers().length -1; i >= 0; i--)
            {
                _logFileLogger.removeHandler(_logFileLogger.getHandlers()[i]);
            }
        }
        //we only want the log handler for this config specified. We remove the other handlers, but keep a reference them still in the _fileHandlers list
        Handler fileHandler = _fileHandlers.get(logFile.toString());
        _logFileLogger.addHandler(fileHandler);
        SimpleFormatter formatter = new SimpleFormatter();
        fileHandler.setFormatter(formatter);
    }

    private void setUpLoggingForConfigs(Map<Path, DataExchangeConfiguration> parsedConfigurations, Path logDirectory)
    {
        List<String> configNameWithoutExtensions = parsedConfigurations.keySet().stream().map(c -> c.getFileName().toString().split("\\.")[0])
                .collect(toList());
        for(String configNameWithoutExtension : configNameWithoutExtensions)
        {
            Path logFile = logDirectory.resolve(configNameWithoutExtension + ".log");
            try
            {
                Files.createDirectories(logDirectory);
                if(logFile.toFile().exists())
                {
                    Path bakFile = Paths.get(logFile.toString().replaceAll(LOG_PATTERN.pattern(), ".bak"));
                    Files.deleteIfExists(bakFile);
                    Files.copy(logFile, bakFile);
                    Files.delete(logFile);
                }
                Files.createFile(logFile);
                FileHandler fileHandler = new FileHandler(logFile.toString());
                _fileHandlers.put(logFile.toString(), fileHandler);
                _logFileLogger.setUseParentHandlers(false);
                _logFileLogger.addHandler(fileHandler);
                SimpleFormatter formatter = new SimpleFormatter();
                fileHandler.setFormatter(formatter);
            }
            catch (IOException e)
            {
                LOGGER.log(Level.SEVERE, e, () -> "Failed to create log file: " + logFile);
            }
        }
    }

    private void exchangeDataForSet(DataExchangeSet dataExchangeSet, DataExchangeConfiguration dataExchangeConfig)
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
            exchangeData(dataExchangeConfig, dataExchangeSet, dataStoreSourceOpt.get(), dataStoreDestinationOpt.get());
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

    private void exchangeData(DataExchangeConfiguration config, DataExchangeSet dataExchangeSet, DataStore dataStoreSource, DataStore dataStoreDestination)
    {
        TemplateWrapper template = getTemplateFromDataExchangeSet(config, dataExchangeSet);
        if(template != null)
        {
            String unitSystemToConvertTo = dataExchangeSet.getUnitSystem();
            logProgress("Specified unit system: " + unitSystemToConvertTo);
            try
            {
                DataExchangeReader reader = DataExchangeReaderFactory.lookupReader(dataStoreSource, _logFileLogger);
                DataExchangeWriter writer = DataExchangeWriterFactory.lookupWriter(dataStoreDestination, _logFileLogger);
                reader.initialize(dataStoreSource, _runtimeParameters);
                writer.initialize(dataStoreDestination, _runtimeParameters);
                String sourcePath = dataStoreSource.getPath(); //for merlin this is a URL
                DataExchangeCache cache = _dataExchangeCache.get(new ApiConnectionInfo(sourcePath));
                if(cache != null)
                {
                    List<MeasureWrapper> measures = cache.getCachedTemplateToMeasures().get(template);
                    List<CompletableFuture<Void>> measurementFutures = new ArrayList<>();
                    try
                    {
                        measures.forEach(measure ->
                                measurementFutures.add(DataExchangeIO.exchangeData(reader, writer, dataExchangeSet, _runtimeParameters, cache,
                                        measure, _completionTracker, _progressListener, _isCancelled, _logFileLogger, _executorService)));
                        CompletableFuture.allOf(measurementFutures.toArray(new CompletableFuture[0])).join();
                    }
                    finally
                    {
                        reader.close();
                        writer.close();
                    }
                }
            }
            catch (DataExchangeLookupException e)
            {
                logError("Lookup failed!", e);
            }
        }
        else
        {
            String errorMsg = "Failed to find matching template ID in retrieved templates for template name " + dataExchangeSet.getTemplateName() + " or id " + dataExchangeSet.getTemplateId();
            logError(errorMsg, null);
        }
    }

    private void logProgress(String message)
    {
        if(_progressListener != null)
        {
            _progressListener.progress(message, MessageType.IMPORTANT);
        }
        _logFileLogger.info(() -> message);
    }

    private void logProgress(String message, int progressPercentage)
    {
        if(_progressListener != null)
        {
            _progressListener.progress(message, MessageType.IMPORTANT, progressPercentage);
        }
        _logFileLogger.info(() -> message);
    }

    private void logError(String message, Throwable error)
    {
        if(_progressListener != null)
        {
            _progressListener.progress(message, MessageType.ERROR);
        }
        _logFileLogger.severe(() -> message);
        if(error != null)
        {
            LOGGER.log(Level.CONFIG, () -> message);
        }
        else
        {
            LOGGER.log(Level.CONFIG, error, () -> message);
        }
    }

    private List<MeasureWrapper> retrieveMeasures(ApiConnectionInfo connectionInfo, TokenContainer accessToken, TemplateWrapper template)
    {
        List<MeasureWrapper> retVal = new ArrayList<>();
        try
        {
            retVal = _merlinDataAccess.getMeasurementsByTemplate(connectionInfo, accessToken, template);
        }
        catch (HttpAccessException | IOException ex)
        {
            logError("Unable to access the merlin web services to retrieve measures for template " + template, ex);
        }
        return retVal;
    }

    private TemplateWrapper getTemplateFromDataExchangeSet(DataExchangeConfiguration dataExchangeConfiguration, DataExchangeSet dataExchangeSet)
    {
        String sourcePath = getDataStoreSourcePath(dataExchangeConfiguration, dataExchangeSet);
        String templateNameFromSet = dataExchangeSet.getTemplateName();
        int templateIdFromSet = dataExchangeSet.getTemplateId();
        DataExchangeCache cache = _dataExchangeCache.get(new ApiConnectionInfo(sourcePath));
        TemplateWrapper retVal = null;
        if(cache != null)
        {
            List<TemplateWrapper> cachedTemplates = cache.getCachedTemplates();
            retVal = cachedTemplates.stream()
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

    private void initializeCacheForMerlinWithToken(Map<Path, DataExchangeConfiguration> parsedConfigurations, Path logFileDirectory, ApiConnectionInfo connectionInfo, TokenContainer token)
            throws IOException, HttpAccessException
    {

        DataExchangeCache cache = _dataExchangeCache.get(connectionInfo);
        if(cache == null)
        {
            cache = new DataExchangeCache();
            _dataExchangeCache.put(connectionInfo, cache);
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
                initializeCachedMeasurementsForMerlin(cache, parsedConfigurations, logFileDirectory, connectionInfo, token);
            }
        }
        else
        {
            initializeCachedMeasurementsForMerlin(cache, parsedConfigurations, logFileDirectory, connectionInfo, token);
        }
    }

    private void initializeCachedMeasurementsForMerlin(DataExchangeCache cache, Map<Path, DataExchangeConfiguration> parsedConfigurations, Path logFileDirectory,
                                                       ApiConnectionInfo connectionInfo, TokenContainer token)
    {
        logProgress("Caching measurements for templates in configs...");
        for(Map.Entry<Path, DataExchangeConfiguration> entry : parsedConfigurations.entrySet())
        {
            DataExchangeConfiguration dataExchangeConfig = entry.getValue();
            List<DataExchangeSet> exchangeSets = dataExchangeConfig.getDataExchangeSets();
            exchangeSets.forEach(set ->
            {
                TemplateWrapper template = getTemplateFromDataExchangeSet(dataExchangeConfig, set);
                boolean alreadyCached = cache.getCachedTemplateToMeasures().containsKey(template);
                if(template != null)
                {
                    List<MeasureWrapper> measures;
                    if(!alreadyCached)
                    {
                        measures = retrieveMeasures(connectionInfo, token, template);
                        cache.cacheSeriesIds(template, measures);
                    }
                    else
                    {
                        measures = cache.getCachedTemplateToMeasures().get(template);
                    }
                    _completionTracker.addNumberOfMeasuresToComplete(measures.size());
                }
            });
        }
        logProgress("Measurements successfully cached!", PERCENT_COMPLETE_ALLOCATED_FOR_INITIAL_SETUP);
    }

}
