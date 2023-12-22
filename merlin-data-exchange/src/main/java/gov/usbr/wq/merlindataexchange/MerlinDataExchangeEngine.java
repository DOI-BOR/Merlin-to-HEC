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
import gov.usbr.wq.merlindataexchange.io.QualityVersionFromSetUtil;
import gov.usbr.wq.merlindataexchange.io.wq.MerlinDataExchangeProfileReader;
import gov.usbr.wq.merlindataexchange.parameters.AuthenticationParameters;
import gov.usbr.wq.merlindataexchange.parameters.MerlinParameters;
import gov.usbr.wq.merlindataexchange.parameters.UsernamePasswordHolder;
import gov.usbr.wq.merlindataexchange.parameters.UsernamePasswordNotFoundException;
import hec.ui.ProgressListener;
import hec.ui.ProgressListener.MessageType;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.toList;

public final class MerlinDataExchangeEngine<P extends MerlinParameters> extends MerlinEngine implements DataExchangeEngine
{
    private static final Logger LOGGER = Logger.getLogger(MerlinDataExchangeEngine.class.getName());
    private static final int PERCENT_COMPLETE_ALLOCATED_FOR_INITIAL_SETUP = 5;
    public static final String READ_WRITE_TIMESTAMP_PROPERTY = "merlin.dataexchange.readwrite.log.timestamptoggle";
    private final Map<ApiConnectionInfo, DataExchangeCache> _dataExchangeCache = new HashMap<>();
    private final MerlinTimeSeriesDataAccess _merlinDataAccess = new MerlinTimeSeriesDataAccess();
    private final AtomicBoolean _isCancelled = new AtomicBoolean(false);
    private final List<Path> _configurationFiles;
    private final P _runtimeParameters;
    private final ProgressListener _progressListener;
    private final MerlinExchangeCompletionTracker _completionTracker = new MerlinExchangeCompletionTracker(PERCENT_COMPLETE_ALLOCATED_FOR_INITIAL_SETUP);
    private final Map<Path, MerlinDataExchangeLogger> _fileLoggers = new HashMap<>();
    private CompletableFuture<MerlinDataExchangeStatus> _extractFuture;
    private Instant _extractStart;

    MerlinDataExchangeEngine(List<Path> configurationFiles, P runtimeParameters, ProgressListener progressListener)
    {
        _configurationFiles = configurationFiles;
        _runtimeParameters = runtimeParameters;
        _progressListener = progressListener;
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
                startStr = MerlinLogDateFormatter.formatInstant(start);
            }
            String endStr = "NULL";
            Instant end = _runtimeParameters.getEnd();
            if(end != null)
            {
                endStr = MerlinLogDateFormatter.formatInstant(end);
            }
            String timeWindowMsg = "Time Window: " + startStr + " | " + endStr;
            String performedOnMsg = "Extract Performed on: " + MerlinLogDateFormatter.formatInstant(_extractStart);
            logImportantProgress(performedOnMsg);
            logImportantProgress(timeWindowMsg);
            Path watershedDirectory = _runtimeParameters.getWatershedDirectory();
            Path logFileDirectory = _runtimeParameters.getLogFileDirectory();
            List<AuthenticationParameters> authParams = _runtimeParameters.getAuthenticationParameters();
            String studyDirMsg = "Study directory: " + watershedDirectory.toString();
            String logFileFirMsg = "Log file directory: " + logFileDirectory.toString();

            setUpLoggingForConfigs(_configurationFiles, _runtimeParameters.getLogFileDirectory());
            for (MerlinDataExchangeLogger merlinDataExchangeLogger : _fileLoggers.values())
            {
                merlinDataExchangeLogger.logToHeader(performedOnMsg);
                merlinDataExchangeLogger.logToHeader(timeWindowMsg);
                merlinDataExchangeLogger.logToHeader(studyDirMsg);
                merlinDataExchangeLogger.logToHeader(logFileFirMsg);
                _runtimeParameters.logAdditionalParameters(merlinDataExchangeLogger);
                for (AuthenticationParameters authParam : authParams)
                {
                    merlinDataExchangeLogger.logToHeader("Username for " + authParam.getUrl() + ": " + authParam.getUsername());
                }
            }
            MerlinDataExchangeStatus retVal = MerlinDataExchangeStatus.FAILURE;
            try
            {
                Map<Path, DataExchangeConfiguration> parsedConfigurations = parseConfigurations();
                List<ApiConnectionInfo> merlinRoots = getMerlinUrlPaths(parsedConfigurations.values());
                for(ApiConnectionInfo connectionInfo : merlinRoots)
                {
                    initializeCacheForMerlinUrl(connectionInfo, parsedConfigurations);
                }
                if(!_isCancelled.get())
                {
                    extractUsingInitializedCache(parsedConfigurations);
                }
                retVal = _completionTracker.getCompletionStatus();
            }
            catch (MerlinConfigParseException | MerlinInitializationException e)
            {
                String errorMsg = e.getMessage();
                logError(errorMsg, e);
                MerlinDataExchangeLogBody body = new MerlinDataExchangeLogBody();
                body.log(errorMsg);
                _fileLoggers.values().forEach(fl -> fl.logBody(body));
            }
            catch (MerlinAuthorizationException e)
            {
                retVal = MerlinDataExchangeStatus.AUTHENTICATION_FAILURE;
                String errorMsg = e.getMessage();
                logError(errorMsg, e);
                MerlinDataExchangeLogBody bodyWithError = new MerlinDataExchangeLogBody();
                bodyWithError.log(errorMsg);
                _fileLoggers.values().forEach(logger -> logger.logBody(bodyWithError));
            }
            logCompletion(retVal);
            _fileLoggers.values().forEach(MerlinDataExchangeLogger::writeLog);
            finish();
            return retVal;
        }, getExecutorService());
    }

    private void logCompletion(MerlinDataExchangeStatus retVal)
    {
        Instant endTime = Instant.now();
        String finishedTimeMsg = "Ended at: " + MerlinLogDateFormatter.formatInstant(endTime);
        String formattedDurationMsg = "Total Duration: " + MerlinDurationFormatter.getFormattedDuration(_extractStart, endTime);
        Collection<MerlinDataExchangeLogger> fileLoggers = _fileLoggers.values();
        if(_isCancelled.get())
        {
            fileLoggers.forEach(logger -> logger.logToFooter("Extract Cancelled | " + finishedTimeMsg + " | " + formattedDurationMsg));
            logCompletionProgress("Extract Cancelled | " + finishedTimeMsg + " | " + formattedDurationMsg);
        }
        else if(retVal == MerlinDataExchangeStatus.PARTIAL_SUCCESS)
        {
            fileLoggers.forEach(logger -> logger.logToFooter("Extract Completed Partially | " + finishedTimeMsg + " | " + formattedDurationMsg));
            logCompletionProgress("Extract Completed Partially | " + finishedTimeMsg + " | " + formattedDurationMsg);
        }
        else if(retVal == MerlinDataExchangeStatus.COMPLETE_SUCCESS)
        {
            fileLoggers.forEach(logger -> logger.logToFooter("Extract Completed! | " + finishedTimeMsg + " | " + formattedDurationMsg));
            logCompletionProgress("Extract Completed! | " + finishedTimeMsg + " | " + formattedDurationMsg);
        }
        else
        {
            fileLoggers.forEach(logger -> logger.logToFooter("Extract Failed! | " + finishedTimeMsg + " | " + formattedDurationMsg));
            logCompletionProgress("Extract Failed! | " + finishedTimeMsg + " | " + formattedDurationMsg);
        }
    }

    private void logCompletionProgress(String completionMessage)
    {
        if (_progressListener != null)
        {
            _progressListener.progress(completionMessage, MessageType.IMPORTANT);
        }
    }

    private void initializeCacheForMerlinUrl(ApiConnectionInfo connectionInfo, Map<Path, DataExchangeConfiguration> parsedConfiguartions)
            throws MerlinAuthorizationException, MerlinInitializationException
    {
        try
        {
            UsernamePasswordHolder usernamePassword = _runtimeParameters.getUsernamePasswordForUrl(connectionInfo.getApiRoot());
            initializeCacheForMerlinUrlWithAuthentication(connectionInfo, parsedConfiguartions, usernamePassword);
        }
        catch (UsernamePasswordNotFoundException e)
        {
            throw new MerlinInitializationException(connectionInfo, e);
        }
    }

    private void initializeCacheForMerlinUrlWithAuthentication(ApiConnectionInfo connectionInfo, Map<Path, DataExchangeConfiguration> parsedConfiguartions, UsernamePasswordHolder usernamePassword)
            throws MerlinAuthorizationException, MerlinInitializationException
    {
        TokenContainer token;
        try
        {
            token = HttpAccessUtils.authenticate(connectionInfo, usernamePassword.getUsername(), usernamePassword.getPassword());
        }
        catch (HttpAccessException e)
        {
            throw new MerlinAuthorizationException(e, usernamePassword, connectionInfo);
        }
        try
        {
            initializeCacheForMerlinWithToken(parsedConfiguartions, connectionInfo, token);
        }
        catch (IOException | HttpAccessException e)
        {
            throw new MerlinInitializationException(connectionInfo, e);
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
            logImportantProgress(logMessage);
            String logFileMessage = "Logging extract to: " + logFileLogger.getLogFile();
            logImportantProgress(logFileMessage);
            MerlinDataExchangeLogBody logBody = new MerlinDataExchangeLogBody();
            logBody.log(logMessage);
            logFileLogger.logBody(logBody);
            DataExchangeConfiguration dataExchangeConfiguration = entry.getValue();
            extractConfiguration(dataExchangeConfiguration, logFileLogger);
            String finishedMsg = "Finished extract for configuration file: " + configPath;
            MerlinDataExchangeLogBody finishedBody = new MerlinDataExchangeLogBody();
            finishedBody.log(finishedMsg);
            logFileLogger.logBody(finishedBody);
            logImportantProgress(finishedMsg);
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

    private Map<Path, DataExchangeConfiguration> parseConfigurations() throws MerlinConfigParseException
    {
        Map<Path, DataExchangeConfiguration> retVal = new TreeMap<>();
        for (Path configFilepath : _configurationFiles)
        {
            retVal.put(configFilepath, parseDataExchangeConfiguration(configFilepath));
        }
        logGeneralProgress("Read configuration files", 1);
        return retVal;
    }

    private void extractConfiguration(DataExchangeConfiguration dataExchangeConfig, MerlinDataExchangeLogger logFileLogger)
    {
        if (dataExchangeConfig != null)
        {
            List<DataExchangeSet> dataExchangeSets = dataExchangeConfig.getDataExchangeSets();
            dataExchangeSets.forEach(dataExchangeSet ->
            {
                if(!_isCancelled.get() && _runtimeParameters.supportsDataExchangeSet(dataExchangeSet))
                {
                    MerlinDataExchangeLogBody logBody = new MerlinDataExchangeLogBody();
                    exchangeDataForSet(dataExchangeSet, dataExchangeConfig, logBody);
                    logFileLogger.logBody(logBody);
                }
            });
        }
    }

    private void setUpLoggingForConfigs(List<Path> configurationPaths, Path logDirectory)
    {
        for(Path configPath : configurationPaths)
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

    @SuppressWarnings("unchecked")
    private void exchangeData(DataExchangeSet dataExchangeSet, DataStore dataStoreSource, DataStore dataStoreDestination, MerlinDataExchangeLogBody logBody)
    {
        try
        {
            TemplateWrapper template = getTemplateFromDataExchangeSet(dataExchangeSet, dataStoreSource);
            DataExchangeReader<P,?> reader = (DataExchangeReader<P, ?>) DataExchangeReaderFactory.lookupReader(dataStoreSource, dataExchangeSet);
            DataExchangeWriter<P,?> writer = (DataExchangeWriter<P, ?>) DataExchangeWriterFactory.lookupWriter(dataStoreDestination, dataExchangeSet);
            String sourcePath = dataStoreSource.getPath(); //for merlin this is a URL
            DataExchangeCache cache = _dataExchangeCache.get(new ApiConnectionInfo(sourcePath));
            if(cache != null)
            {
                String fromMsg = "Reading from: " + reader.getSourcePath(dataStoreSource, _runtimeParameters);
                String toMsg = "Writing to: " + writer.getDestinationPath(dataStoreDestination, _runtimeParameters);
                String templateMsg = dataExchangeSet.getTemplateName();
                if(templateMsg == null)
                {
                    templateMsg = "" + dataExchangeSet.getTemplateId();
                }
                String qualityVersionMsg = dataExchangeSet.getQualityVersionName();
                if(qualityVersionMsg == null)
                {
                    qualityVersionMsg = "" + dataExchangeSet.getQualityVersionId();
                }
                String detailsMsg = "Template: " + templateMsg + " | Specified quality version: " + qualityVersionMsg
                        + " | Specified unit system: " + dataExchangeSet.getUnitSystem();
                logBody.log(fromMsg);
                logBody.log(toMsg);
                logBody.log(detailsMsg);
                logGeneralProgress(fromMsg);
                logGeneralProgress(toMsg);
                logGeneralProgress(detailsMsg);
                if(template == null)
                {
                    template = getTemplateFromDataExchangeSet(dataExchangeSet, dataStoreDestination);
                }
                if(template == null)
                {
                    throw new UnsupportedTemplateException(dataExchangeSet.getTemplateName(), dataExchangeSet.getTemplateId());
                }
                Optional<QualityVersionWrapper> qualityVersion = QualityVersionFromSetUtil.getQualityVersionIdFromDataExchangeSet(dataExchangeSet, cache);
                if(!qualityVersion.isPresent())
                {
                    throw new UnsupportedQualityVersionException(dataExchangeSet.getQualityVersionName(), dataExchangeSet.getQualityVersionId());
                }
                List<MeasureWrapper> measures = cache.getCachedTemplateToMeasures().get(template);
                measures = reader.filterMeasuresToRead(dataExchangeSet, measures);
                List<CompletableFuture<Void>> measurementFutures = new ArrayList<>();
                measures.forEach(measure ->
                        measurementFutures.add(DataExchangeIO.exchangeData(reader, writer, dataExchangeSet, _runtimeParameters, dataStoreSource, dataStoreDestination,
                                cache, measure, _completionTracker, _progressListener, _isCancelled, logBody, getExecutorService())));
                CompletableFuture.allOf(measurementFutures.toArray(new CompletableFuture[0])).join();
            }
        }
        catch (DataExchangeLookupException | UnsupportedTemplateException | UnsupportedQualityVersionException e)
        {
            _completionTracker.addNumberOfMeasuresToComplete(1);
            logError(e.getMessage(), e);
            logBody.log(e.getMessage());
        }
    }

    private void logImportantProgress(String message)
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

    private void logGeneralProgress(String message)
    {
        if(!_isCancelled.get())
        {
            if(_progressListener != null)
            {
                _progressListener.progress(message, MessageType.GENERAL);
            }
            LOGGER.fine(() -> message);
        }

    }

    private void logGeneralProgress(String message, int progressPercentage)
    {
        if(!_isCancelled.get())
        {
            if(_progressListener != null)
            {
                _progressListener.progress(message, MessageType.GENERAL, progressPercentage);
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

    private TemplateWrapper getTemplateFromDataExchangeSet(DataExchangeSet dataExchangeSet, DataStore dataStore)
    {
        TemplateWrapper retVal = null;
        if(dataStore != null)
        {
            String dataStorePath = dataStore.getPath();
            String templateNameFromSet = dataExchangeSet.getTemplateName();
            Integer templateIdFromSet = dataExchangeSet.getTemplateId();
            DataExchangeCache cache = _dataExchangeCache.get(new ApiConnectionInfo(dataStorePath));

            if (cache != null)
            {
                List<TemplateWrapper> cachedTemplates = cache.getCachedTemplates();
                retVal = cachedTemplates.stream()
                        .filter(template -> template.getName().equalsIgnoreCase(templateNameFromSet))
                        .findFirst()
                        .orElse(null);
                if (retVal == null)
                {
                    retVal = cachedTemplates.stream()
                            .filter(template -> Objects.equals(template.getDprId(), templateIdFromSet))
                            .findFirst()
                            .orElse(null);
                }
            }
        }
        return retVal;
    }

    private DataExchangeConfiguration parseDataExchangeConfiguration(Path xmlConfigurationFile) throws MerlinConfigParseException
    {
        DataExchangeConfiguration retVal = MerlinDataExchangeParser.parseXmlFile(xmlConfigurationFile);
        logImportantProgress("Read configuration file: " + xmlConfigurationFile);
        return retVal;
    }

    private void initializeCacheForMerlinWithToken(Map<Path, DataExchangeConfiguration> parsedConfigurations, ApiConnectionInfo connectionInfo, TokenContainer token)
            throws IOException, HttpAccessException, MerlinInitializationException
    {

        DataExchangeCache cache = _dataExchangeCache.get(connectionInfo);
        if(cache == null)
        {
            cache = new DataExchangeCache();
            _dataExchangeCache.put(connectionInfo, cache);
            List<TemplateWrapper> templates = _merlinDataAccess.getTemplates(connectionInfo, token);
            cache.cacheTemplates(templates);
            logGeneralProgress("Retrieved " + templates.size() + " templates", (int) (PERCENT_COMPLETE_ALLOCATED_FOR_INITIAL_SETUP * 0.4));
            List<QualityVersionWrapper> qualityVersions = _merlinDataAccess.getQualityVersions(connectionInfo, token);
            cache.cacheQualityVersions(qualityVersions);
            logGeneralProgress("Retrieved " + qualityVersions.size() + " quality versions", (int) (PERCENT_COMPLETE_ALLOCATED_FOR_INITIAL_SETUP * 0.6));
            if(!_isCancelled.get())
            {
                initializeCachedMeasurementsForMerlin(cache, parsedConfigurations, connectionInfo, token);
            }
        }
        else
        {
            initializeCachedMeasurementsForMerlin(cache, parsedConfigurations, connectionInfo, token);
        }
        for(DataExchangeConfiguration config : parsedConfigurations.values())
        {
            //get template for each set and collect into list
            List<TemplateWrapper> templatesFromConfig = config.getDataExchangeSets().stream()
                    .map(set -> getTemplateFromDataExchangeSet(set, getSourceDataStoreForSet(config, set)))
                    .collect(toList());
            validateProjects(cache, connectionInfo, templatesFromConfig);
        }

    }

    private DataStore getSourceDataStoreForSet(DataExchangeConfiguration dataExchangeConfig, DataExchangeSet dataExchangeSet)
    {
        DataStoreRef dataStoreRefB = dataExchangeSet.getDataStoreRefB();
        DataStoreRef sourceRef = dataExchangeSet.getDataStoreRefA();
        if (dataExchangeSet.getSourceId().equalsIgnoreCase(dataStoreRefB.getId()))
        {
            sourceRef = dataStoreRefB;
        }
        return dataExchangeConfig.getDataStoreByRef(sourceRef).orElse(null);
    }

    void validateProjects(DataExchangeCache cache, ApiConnectionInfo connectionInfo, List<TemplateWrapper> templatesFromConfig) throws MerlinInitializationException
    {
        for(TemplateWrapper template : templatesFromConfig)
        {
            List<MeasureWrapper> measures = cache.getCachedTemplateToMeasures().get(template);
            if(measures != null && !measures.isEmpty())
            {
                String projSiteSensor = measures.get(0).getProjectAndSiteAndSensor();
                String proj = parseProject(projSiteSensor);
                for(int i=1; i < measures.size(); i++)
                {
                    MeasureWrapper measure = measures.get(i);
                    if(MerlinDataExchangeProfileReader.PROFILE.equalsIgnoreCase(measure.getType()))
                    {
                        String projSiteSensorInList = measure.getProjectAndSiteAndSensor();
                        String projInList = parseProject(projSiteSensorInList);
                        if(!projInList.equals(proj))
                        {
                            throw new MerlinInitializationException(connectionInfo,
                                    "Template " + template.getName() + " has multiple projects. Data Exchange does not currently support multiple projects in a single template for profiles");
                        }
                    }
                }
            }
        }
    }

    private String parseProject(String input)
    {
        int lastIndex = input.lastIndexOf("-");
        if (lastIndex == -1)
        {
            return input; // No "-" found, return input
        }
        if(input.toLowerCase().endsWith("temp-water"))
        {
            input = input.substring(0, lastIndex);
            lastIndex = input.lastIndexOf("-");
        }

        int secondToLastIndex = input.lastIndexOf("-", lastIndex - 1);
        String retVal;
        if (secondToLastIndex == -1)
        {
            retVal = input.substring(0, lastIndex); // Only one "-" found, return everything before it
        }
        else
        {
            retVal = input.substring(0, secondToLastIndex).trim(); // Return everything before the second to last "-" in case site contains a "-"
        }

        if (countOccurrences(retVal, '-') > 1)
        {
            retVal = retVal.substring(0, retVal.lastIndexOf("-"));
        }
        return retVal;
    }

    private int countOccurrences(String input, char target)
    {
        int count = 0;
        for (int i = 0; i < input.length(); i++)
        {
            if (input.charAt(i) == target)
            {
                count++;
            }
        }
        return count;
    }

    private void initializeCachedMeasurementsForMerlin(DataExchangeCache cache, Map<Path, DataExchangeConfiguration> parsedConfigurations,
                                                       ApiConnectionInfo connectionInfo, TokenContainer token) throws IOException, HttpAccessException
    {
        for(Map.Entry<Path, DataExchangeConfiguration> entry : parsedConfigurations.entrySet())
        {
            DataExchangeConfiguration dataExchangeConfig = entry.getValue();
            List<DataExchangeSet> exchangeSets = dataExchangeConfig.getDataExchangeSets();
            for(DataExchangeSet set : exchangeSets)
            {
                Optional<TemplateWrapper> templateOpt = cache.getCachedTemplates().stream()
                        .filter(t -> t.getName().equalsIgnoreCase(set.getTemplateName()) || t.getDprId().equals(set.getTemplateId()))
                        .findFirst();
                if(templateOpt.isPresent() && _runtimeParameters.supportsDataExchangeSet(set))
                {
                    TemplateWrapper template = templateOpt.get();
                    boolean alreadyCached = !cache.getCachedTemplateToMeasures().get(template).isEmpty();
                    List<MeasureWrapper> measures;
                    if(!alreadyCached)
                    {
                        measures = _merlinDataAccess.getMeasurementsByTemplate(connectionInfo, token, template);
                        cache.cacheMeasures(template, measures);
                    }
                    else
                    {
                        measures = cache.getCachedTemplateToMeasures().get(template);
                    }
                    _completionTracker.addNumberOfMeasuresToComplete(measures.size());
                }
            }
        }

        logGeneralProgress("Retrieved " + cache.getCachedTemplateToMeasures().values().size() + " measure(s) for template(s): "
                + cache.getCachedTemplateToMeasures().keySet().stream()
                .map(TemplateWrapper::getName)
                .collect(Collectors.joining(", ")), PERCENT_COMPLETE_ALLOCATED_FOR_INITIAL_SETUP);
    }

}
