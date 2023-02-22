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
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
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
import java.util.logging.Level;
import java.util.logging.Logger;

import static java.util.stream.Collectors.toList;

public final class MerlinDataExchangeEngine implements DataExchangeEngine
{
    private static final Logger LOGGER = Logger.getLogger(MerlinDataExchangeEngine.class.getName());
    private static final int PERCENT_COMPLETE_ALLOCATED_FOR_INITIAL_SETUP = 5;
    private static final int THREAD_COUNT = 5;
    private static final String THREAD_PROPERTY_KEY = "merlin.dataexchange.threadpool.size";
    private final ExecutorService _executorService = Executors.newFixedThreadPool(getThreadPoolSize(), new MerlinThreadFactory());
    private final Map<ApiConnectionInfo, DataExchangeCache> _dataExchangeCache = new HashMap<>();
    private final MerlinTimeSeriesDataAccess _merlinDataAccess = new MerlinTimeSeriesDataAccess();
    private final AtomicBoolean _isCancelled = new AtomicBoolean(false);
    private final List<Path> _configurationFiles;
    private final MerlinParameters _runtimeParameters;
    private final ProgressListener _progressListener;
    private final MerlinExchangeCompletionTracker _completionTracker = new MerlinExchangeCompletionTracker(PERCENT_COMPLETE_ALLOCATED_FOR_INITIAL_SETUP);
    private final Map<Path, MerlinDataExchangeLogger> _fileLoggers = new HashMap<>();
    private CompletableFuture<MerlinDataExchangeStatus> _extractFuture;
    private Instant _extractStart;

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
            _extractFuture = beginExtract();
        }
        return _extractFuture;
    }

    @Override
    public void cancelExtract()
    {
        _isCancelled.set(true);
        String cancelMsg = "Merlin Data Exchanged Cancelled. Finishing gracefully...";
        MerlinDataExchangeLogBody body = new MerlinDataExchangeLogBody();
        body.log(cancelMsg);
        _fileLoggers.values().forEach(fl -> fl.logBody(body));
        if(_progressListener != null)
        {
            _progressListener.progress(cancelMsg);
        }
    }

    private CompletableFuture<MerlinDataExchangeStatus> beginExtract()
    {
        return CompletableFuture.supplyAsync(() ->
        {
            _extractStart = Instant.now();
            String startStr = "NULL";
            Instant start = _runtimeParameters.getStart();
            if(start != null)
            {
                startStr = start.toString();
            }
            String endStr = "NULL";
            Instant end = _runtimeParameters.getStart();
            if(end != null)
            {
                endStr = end.toString();
            }
            String timeWindowMsg = "Time Window: " + startStr + " | " + endStr;
            String performedOnMsg = "Extract Performed on: " + _extractStart;
            logProgress(performedOnMsg);
            logProgress(timeWindowMsg);
            Map<Path, DataExchangeConfiguration> parsedConfigurations = parseConfigurations();
            MerlinDataExchangeStatus retVal = MerlinDataExchangeStatus.FAILURE;
            try
            {
                setUpLoggingForConfigs(parsedConfigurations, _runtimeParameters.getLogFileDirectory());
                _fileLoggers.values().forEach(logger ->
                {
                    logger.logToHeader(performedOnMsg);
                    logger.logToHeader(timeWindowMsg);
                });
                logProgress("Setting up extract for all configs...");
                List<ApiConnectionInfo> merlinRoots = getMerlinUrlPaths(parsedConfigurations.values());
                for(ApiConnectionInfo connectionInfo : merlinRoots)
                {
                    initializeCacheForMerlinUrl(connectionInfo, parsedConfigurations);
                }
                if(!_isCancelled.get())
                {
                    logProgress("Setup complete!");
                    logProgress("Running Full Extract...");
                    extractUsingInitializedCache(parsedConfigurations);
                }
                retVal = _completionTracker.getCompletionStatus();
            }
            catch (UnsupportedTemplateException e)
            {
                String errorMsg = "Setup Failed: " + e.getMessage();
                logError(errorMsg, e);
                MerlinDataExchangeLogBody bodyWithError = new MerlinDataExchangeLogBody();
                bodyWithError.log(errorMsg);
                _fileLoggers.values().forEach(logger -> logger.logBody(bodyWithError));
            }
            catch (IOException e)
            {
                logError("Failed to initialize templates, quality versions, and measures cache", e);
                MerlinDataExchangeLogBody bodyWithError = new MerlinDataExchangeLogBody();
                bodyWithError.log("Error Occurred: " + e.getMessage());
                _fileLoggers.values().forEach(logger -> logger.logBody(bodyWithError));
            }
            logCompletion(retVal);
            _fileLoggers.values().forEach(MerlinDataExchangeLogger::writeLog);
            finish();
            return retVal;
        }, _executorService);
    }

    private void logCompletion(MerlinDataExchangeStatus retVal)
    {
        Collection<MerlinDataExchangeLogger> fileLoggers = _fileLoggers.values();
        if(_isCancelled.get())
        {
            fileLoggers.forEach(logger -> logger.logToFooter("Extract Cancelled"));
            logCompletionProgress("Extract Cancelled");
        }
        else if(retVal == MerlinDataExchangeStatus.PARTIAL_SUCCESS)
        {
            fileLoggers.forEach(logger -> logger.logToFooter("Extract Completed Partially"));
            logCompletionProgress("Extract Cancelled");
        }
        else if(retVal == MerlinDataExchangeStatus.COMPLETE_SUCCESS)
        {
            fileLoggers.forEach(logger -> logger.logToFooter("Extract Completed!"));
            logCompletionProgress("Extract Cancelled");
        }
        else
        {
            fileLoggers.forEach(logger -> logger.logToFooter("Extract Failed!"));
            logCompletionProgress("Extract Cancelled");
        }
        String formattedDuration = "Total Duration: " + getFormattedDuration();
        fileLoggers.forEach(logger -> logger.logToFooter(formattedDuration));
        logCompletionProgress(formattedDuration);
    }

    private void logCompletionProgress(String completionMessage)
    {
        if (_progressListener != null)
        {
            _progressListener.progress(completionMessage);
        }
    }

    private String getFormattedDuration()
    {
        String units = " ms";
        double duration = Duration.between(_extractStart, Instant.now()).toMillis();
        if (duration >= 1000)
        {
            duration = duration/1000.0;
            units = " seconds";
        }
        if(duration >= 60)
        {
            duration = duration/60;
            units = " minutes";
        }
        if(duration >= 60)
        {
            duration = duration/60;
            units = " hours";
        }
        if(duration >= 24)
        {
            duration = duration/24;
            units = " days";
        }
        return duration + units;
    }

    private void initializeCacheForMerlinUrl(ApiConnectionInfo connectionInfo, Map<Path, DataExchangeConfiguration> parsedConfiguartions) throws IOException, UnsupportedTemplateException
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
            throws IOException, UnsupportedTemplateException
    {
        try
        {
            TokenContainer token = HttpAccessUtils.authenticate(connectionInfo, usernamePassword.getUsername(), usernamePassword.getPassword());
            initializeCacheForMerlinWithToken(parsedConfiguartions, connectionInfo, token);
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
            MerlinDataExchangeLogger logFileLogger = _fileLoggers.get(configPath);
            String logMessage = "Running Extract for config: " + configPath;
            logProgress(logMessage);
            MerlinDataExchangeLogBody logBody = new MerlinDataExchangeLogBody();
            logBody.log(logMessage);
            DataExchangeConfiguration dataExchangeConfiguration = entry.getValue();
            extractConfiguration(dataExchangeConfiguration, logBody);
            String finishedMsg = "Finished extract for config";
            logBody.log(finishedMsg);
            logFileLogger.logBody(logBody);
            logProgress(finishedMsg);
        }
    }

    private void finish()
    {
        if(_progressListener != null)
        {
            _progressListener.finish();
        }
        _dataExchangeCache.clear();
        _fileLoggers.clear();
        _completionTracker.reset();
    }

    private Map<Path, DataExchangeConfiguration> parseConfigurations()
    {
        Map<Path, DataExchangeConfiguration> retVal = new TreeMap<>();
        _configurationFiles.forEach(configFilepath -> retVal.put(configFilepath, parseDataExchangeConfiguration(configFilepath)));
        logProgress("Configuration files parsed", 1);
        return retVal;
    }

    private void extractConfiguration(DataExchangeConfiguration dataExchangeConfig, MerlinDataExchangeLogBody logBody)
    {
        if (dataExchangeConfig != null)
        {
            List<DataExchangeSet> dataExchangeSets = dataExchangeConfig.getDataExchangeSets();
            dataExchangeSets.forEach(dataExchangeSet ->
            {
                if(!_isCancelled.get())
                {
                    exchangeDataForSet(dataExchangeSet, dataExchangeConfig, logBody);
                }
            });
        }
    }

    private void setUpLoggingForConfigs(Map<Path, DataExchangeConfiguration> parsedConfigurations, Path logDirectory)
    {
        for(Path configPath : parsedConfigurations.keySet())
        {
            String configNameWithoutExtension = configPath.getFileName().toString().split("\\.")[0];
            Path logFile = logDirectory.resolve(configNameWithoutExtension + ".log");
            _fileLoggers.put(configPath, new MerlinDataExchangeLogger(logFile));
        }
    }

    private void exchangeDataForSet(DataExchangeSet dataExchangeSet, DataExchangeConfiguration dataExchangeConfig, MerlinDataExchangeLogBody logBody)
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
            String msg = "No datastore found for datastore-ref id: " + destinationRef.getId();
            logError(msg, null);
            logBody.log("Error occurred: " + msg);
        }
        if(!dataStoreSourceOpt.isPresent())
        {
            String msg = "No datastore found for datastore-ref id: " + sourceRef.getId();
            logError(msg, null);
            logBody.log("Error occurred: " + msg);
        }
        if(dataStoreDestinationOpt.isPresent() && dataStoreSourceOpt.isPresent())
        {
            exchangeData(dataExchangeSet, dataStoreSourceOpt.get(), dataStoreDestinationOpt.get(), logBody);
        }
    }

    private void exchangeData(DataExchangeSet dataExchangeSet, DataStore dataStoreSource, DataStore dataStoreDestination, MerlinDataExchangeLogBody logBody)
    {
        try
        {
            TemplateWrapper template = getTemplateFromDataExchangeSet(dataExchangeSet, dataStoreSource);
            if(template == null)
            {
                template = getTemplateFromDataExchangeSet(dataExchangeSet, dataStoreDestination);
            }
            if(template == null)
            {
                throw new UnsupportedTemplateException(dataExchangeSet.getTemplateName(), dataExchangeSet.getTemplateId());
            }
            DataExchangeReader reader = DataExchangeReaderFactory.lookupReader(dataStoreSource);
            DataExchangeWriter writer = DataExchangeWriterFactory.lookupWriter(dataStoreDestination);
            reader.initialize(dataStoreSource, _runtimeParameters);
            writer.initialize(dataStoreDestination, _runtimeParameters);
            String sourcePath = dataStoreSource.getPath(); //for merlin this is a URL
            DataExchangeCache cache = _dataExchangeCache.get(new ApiConnectionInfo(sourcePath));
            if(cache != null)
            {
                String fromMsg = "From: " + reader.getSourcePath();
                String toMsg = "To: " + writer.getDestinationPath();
                logBody.log(fromMsg);
                logBody.log(toMsg);
                logProgress(fromMsg);
                logProgress(toMsg);
                logProgress("Specified quality version: " + dataExchangeSet.getQualityVersionName());
                logProgress("Specified unit system: " + dataExchangeSet.getUnitSystem());
                List<MeasureWrapper> measures = cache.getCachedTemplateToMeasures().get(template);
                List<CompletableFuture<Void>> measurementFutures = new ArrayList<>();
                try
                {
                    measures.forEach(measure ->
                            measurementFutures.add(DataExchangeIO.exchangeData(reader, writer, dataExchangeSet, _runtimeParameters, cache,
                                    measure, _completionTracker, _progressListener, _isCancelled, logBody, _executorService)));
                    CompletableFuture.allOf(measurementFutures.toArray(new CompletableFuture[0])).join();
                }
                finally
                {
                    reader.close();
                    writer.close();
                }
            }
        }
        catch (DataExchangeLookupException | UnsupportedTemplateException e)
        {
            logError("Lookup failed!", e);
            logBody.log("Error Occurred: " + e.getMessage());
        }
    }

    private void logProgress(String message)
    {
        if(!_isCancelled.get())
        {
            if(_progressListener != null)
            {
                _progressListener.progress(message, MessageType.IMPORTANT);
            }
            LOGGER.fine(() -> message);
        }

    }

    private void logProgress(String message, int progressPercentage)
    {
        if(!_isCancelled.get())
        {
            if(_progressListener != null)
            {
                _progressListener.progress(message, MessageType.IMPORTANT, progressPercentage);
            }
            LOGGER.fine(() -> message);
        }
    }

    private void logError(String message, Throwable error)
    {
        if(!_isCancelled.get())
        {
            if(_progressListener != null)
            {
                _progressListener.progress(message, MessageType.ERROR);
            }
            if(error == null)
            {
                LOGGER.log(Level.CONFIG, () -> message);
            }
            else
            {
                LOGGER.log(Level.CONFIG, error, () -> message + ": " + error.getMessage());
            }
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

    private TemplateWrapper getTemplateFromDataExchangeSet(DataExchangeSet dataExchangeSet, DataStore dataStore)
    {
        String dataStorePath = dataStore.getPath();
        String templateNameFromSet = dataExchangeSet.getTemplateName();
        int templateIdFromSet = dataExchangeSet.getTemplateId();
        DataExchangeCache cache = _dataExchangeCache.get(new ApiConnectionInfo(dataStorePath));
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

    private void initializeCacheForMerlinWithToken(Map<Path, DataExchangeConfiguration> parsedConfigurations, ApiConnectionInfo connectionInfo, TokenContainer token)
            throws IOException, HttpAccessException, UnsupportedTemplateException
    {

        DataExchangeCache cache = _dataExchangeCache.get(connectionInfo);
        if(cache == null)
        {
            cache = new DataExchangeCache();
            _dataExchangeCache.put(connectionInfo, cache);
            logProgress("Caching templates retrieved from Merlin Web Service...");
            List<TemplateWrapper> templates = _merlinDataAccess.getTemplates(connectionInfo, token);
            cache.cacheTemplates(templates);
            logProgress("Successfully cached " + templates.size() + " templates.", (int) (PERCENT_COMPLETE_ALLOCATED_FOR_INITIAL_SETUP * 0.4));
            logProgress("Caching quality versions from Merlin Web Service...");
            List<QualityVersionWrapper> qualityVersions = _merlinDataAccess.getQualityVersions(connectionInfo, token);
            cache.cacheQualityVersions(qualityVersions);
            logProgress("Successfully cached " + qualityVersions.size() + " quality versions.", (int) (PERCENT_COMPLETE_ALLOCATED_FOR_INITIAL_SETUP * 0.6));
            if(!_isCancelled.get())
            {
                initializeCachedMeasurementsForMerlin(cache, parsedConfigurations, connectionInfo, token);
            }
        }
        else
        {
            initializeCachedMeasurementsForMerlin(cache, parsedConfigurations, connectionInfo, token);
        }
    }

    private void initializeCachedMeasurementsForMerlin(DataExchangeCache cache, Map<Path, DataExchangeConfiguration> parsedConfigurations,
                                                       ApiConnectionInfo connectionInfo, TokenContainer token) throws UnsupportedTemplateException
    {
        logProgress("Caching measurements for templates in configs...");
        for(Map.Entry<Path, DataExchangeConfiguration> entry : parsedConfigurations.entrySet())
        {
            DataExchangeConfiguration dataExchangeConfig = entry.getValue();
            List<DataExchangeSet> exchangeSets = dataExchangeConfig.getDataExchangeSets();
            for(DataExchangeSet set : exchangeSets)
            {
                TemplateWrapper template = cache.getCachedTemplates().stream()
                        .filter(t -> t.getName().equals(set.getTemplateName()) || t.getDprId().equals(set.getTemplateId()))
                        .findFirst()
                        .orElseThrow(() -> new UnsupportedTemplateException(set.getTemplateName(), set.getTemplateId()));
                boolean alreadyCached = cache.getCachedTemplateToMeasures().containsKey(template);
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
        }
        logProgress("Measurements successfully cached!", PERCENT_COMPLETE_ALLOCATED_FOR_INITIAL_SETUP);
    }

}
