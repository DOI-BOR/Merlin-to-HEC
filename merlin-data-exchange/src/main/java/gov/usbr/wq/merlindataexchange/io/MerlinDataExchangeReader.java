package gov.usbr.wq.merlindataexchange.io;

import gov.usbr.wq.dataaccess.MerlinTimeSeriesDataAccess;
import gov.usbr.wq.dataaccess.http.ApiConnectionInfo;
import gov.usbr.wq.dataaccess.http.HttpAccessException;
import gov.usbr.wq.dataaccess.http.HttpAccessUtils;
import gov.usbr.wq.dataaccess.http.TokenContainer;
import gov.usbr.wq.dataaccess.model.DataWrapper;
import gov.usbr.wq.dataaccess.model.MeasureWrapper;
import gov.usbr.wq.dataaccess.model.QualityVersionWrapper;
import gov.usbr.wq.merlindataexchange.DataExchangeCache;
import gov.usbr.wq.merlindataexchange.parameters.MerlinParameters;
import gov.usbr.wq.merlindataexchange.MerlinExchangeDaoCompletionTracker;
import gov.usbr.wq.merlindataexchange.parameters.UsernamePasswordHolder;
import gov.usbr.wq.merlindataexchange.parameters.UsernamePasswordNotFoundException;
import gov.usbr.wq.merlindataexchange.configuration.DataExchangeSet;
import gov.usbr.wq.merlindataexchange.configuration.DataStore;
import gov.usbr.wq.merlintohec.exceptions.MerlinInvalidTimestepException;
import gov.usbr.wq.merlintohec.model.MerlinDaoConversionUtil;
import hec.io.TimeSeriesContainer;
import hec.ui.ProgressListener;
import hec.ui.ProgressListener.MessageType;
import rma.services.annotations.ServiceProvider;

import java.io.IOException;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

@ServiceProvider(service = DataExchangeReader.class, position = 100, path = DataExchangeReader.LOOKUP_PATH
        + "/" + MerlinDataExchangeReader.MERLIN)
public final class MerlinDataExchangeReader implements DataExchangeReader
{

    public static final String MERLIN = "merlin";
    private static final Logger LOGGER = Logger.getLogger(MerlinDataExchangeReader.class.getName());
    private String _merlinApiRoot;
    private TokenContainer _token;

    @Override
    public CompletableFuture<TimeSeriesContainer> readData(DataExchangeSet dataExchangeSet, MerlinParameters runtimeParameters, DataExchangeCache cache, MeasureWrapper measure,
                                                           MerlinExchangeDaoCompletionTracker completionTracker, ProgressListener progressListener, AtomicBoolean isCancelled,
                                                           Logger logFileLogger, ExecutorService executorService)
    {
        Instant start = runtimeParameters.getStart();
        Instant end = runtimeParameters.getEnd();
        String fPartOverride = runtimeParameters.getFPartOverride();
        QualityVersionWrapper qualityVersion = getQualityVersionIdFromDataExchangeSet(dataExchangeSet, cache, logFileLogger, progressListener).orElse(null);
        String unitSystemToConvertTo = dataExchangeSet.getUnitSystem();
        Integer qualityVersionId = qualityVersion == null ? null : qualityVersion.getQualityVersionID();
        String seriesPath = measure.getSeriesString();
        logFileLogger.info(() -> "Start time: " + start + " | End time: " + end + " | Quality version: " + qualityVersionId);
        logFileLogger.info(() -> "Retrieving data for measure with series string: " + seriesPath + "...");
        if(progressListener != null)
        {
            progressListener.progress("Retrieving data for measure with series string: " + seriesPath + "...", MessageType.IMPORTANT);
        }
        return CompletableFuture.supplyAsync(() ->
        {
            TimeSeriesContainer retVal = null;
            try
            {
                UsernamePasswordHolder usernamePassword = runtimeParameters.getUsernamePasswordForUrl(_merlinApiRoot);
                retVal = retrieveDataAsTimeSeries(usernamePassword, start, end, measure, qualityVersionId, completionTracker, progressListener, logFileLogger,
                        isCancelled, fPartOverride, unitSystemToConvertTo);
            }
            catch (UsernamePasswordNotFoundException e)
            {
                String errorMsg = "Failed to find username/password in parameters for URL: " + _merlinApiRoot;
                logError(progressListener, logFileLogger, errorMsg, e);
            }
            return retVal;

        }, executorService);
    }

    private TimeSeriesContainer retrieveDataAsTimeSeries(UsernamePasswordHolder usernamePassword, Instant start, Instant end, MeasureWrapper measure, Integer qualityVersionId,
                                                         MerlinExchangeDaoCompletionTracker completionTracker, ProgressListener progressListener, Logger logFileLogger,
                                                         AtomicBoolean isCancelled, String fPartOverride, String unitSystemToConvertTo)
    {
        TimeSeriesContainer retVal = null;
        try
        {
            _token = generateNewTokenIfNecessary(new ApiConnectionInfo(_merlinApiRoot), usernamePassword.getUsername(), usernamePassword.getPassword());
            DataWrapper data = retrieveData(start, end, measure, qualityVersionId,
                    completionTracker, progressListener, logFileLogger, isCancelled);
            if(!isCancelled.get())
            {
                retVal = convertToTsc(data, unitSystemToConvertTo, fPartOverride, progressListener, logFileLogger);
            }
        }
        catch (HttpAccessException e)
        {
            String errorMsg = "Failed to authenticate user: " + usernamePassword.getUsername();
            logError(progressListener, logFileLogger, errorMsg, e);
        }
        return retVal;
    }

    private TimeSeriesContainer convertToTsc(DataWrapper data, String unitSystemToConvertTo, String fPartOverride, ProgressListener progressListener, Logger logFileLogger)
    {
        TimeSeriesContainer retVal = null;
        try
        {
            retVal = MerlinDaoConversionUtil.convertToTsc(data, unitSystemToConvertTo, fPartOverride, progressListener, logFileLogger);
        }
        catch (MerlinInvalidTimestepException e)
        {
            logFileLogger.log(Level.WARNING, e, () -> "Unsupported timestep: " + data.getTimestep());
            if(progressListener != null)
            {
                progressListener.progress("Skipping Measure with unsupported timestep", MessageType.IMPORTANT);
            }
            LOGGER.log(Level.CONFIG, e, () -> "Unsupported timestep: " + data.getTimestep());
        }
        return retVal;
    }

    private TokenContainer generateNewTokenIfNecessary(ApiConnectionInfo connectionInfo, String username, char[] password) throws HttpAccessException
    {
        if(_token == null || _token.isExpired())
        {
            _token = HttpAccessUtils.authenticate(connectionInfo, username, password);
        }
        return _token;
    }

    @Override
    public void initialize(DataStore dataStore, MerlinParameters parameters)
    {
        _merlinApiRoot = dataStore.getPath();
    }

    @Override
    public void close()
    {
        //Nothing to close from our end, reading from merlin web service
    }

    @Override
    public String getSourcePath()
    {
        return _merlinApiRoot;
    }

    private DataWrapper retrieveData(Instant start, Instant end, MeasureWrapper measure, Integer qualityVersionId,
                                     MerlinExchangeDaoCompletionTracker completionTracker,
                                     ProgressListener progressListener, Logger logFileLogger, AtomicBoolean isCancelled)
    {
        MerlinTimeSeriesDataAccess access = new MerlinTimeSeriesDataAccess();
        DataWrapper retVal = null;
        if(!isCancelled.get())
        {
            try
            {
                retVal = access.getEventsBySeries(new ApiConnectionInfo(_merlinApiRoot), _token, measure, qualityVersionId, start, end);
                String progressMsg = "Successfully retrieved data for " + measure.getSeriesString() + " with " + retVal.getEvents().size() + " events!";
                int percentComplete = completionTracker.readTaskCompleted();
                logProgress(progressListener, logFileLogger, progressMsg, percentComplete);
            }
            catch (IOException | HttpAccessException ex)
            {
                logError(progressListener, logFileLogger, "Failed to retrieve data for measure with series string: " + measure.getSeriesString(), ex);
            }
        }
        return retVal;
    }

    private Optional<QualityVersionWrapper> getQualityVersionIdFromDataExchangeSet(DataExchangeSet dataExchangeSet, DataExchangeCache cache, Logger logger, ProgressListener progressListener)
    {
        String qualityVersionNameFromSet = dataExchangeSet.getQualityVersionName();
        Integer qualityVersionIdFromSet = dataExchangeSet.getQualityVersionId();
        if(progressListener != null)
        {
            progressListener.progress("Retrieving Quality Version for " + qualityVersionNameFromSet + " (id: " + qualityVersionIdFromSet + ")", ProgressListener.MessageType.IMPORTANT);
        }
        List<QualityVersionWrapper> qualityVersions = cache.getCachedQualityVersions();
        Optional<QualityVersionWrapper> retVal = qualityVersions.stream()
                .filter(qualityVersion -> qualityVersion.getQualityVersionName().equalsIgnoreCase(qualityVersionNameFromSet))
                .findFirst();
        if(!retVal.isPresent())
        {
            retVal = qualityVersions.stream()
                    .filter(qualityVersion -> qualityVersion.getQualityVersionID().intValue() == qualityVersionIdFromSet)
                    .findFirst();
        }
        if(!retVal.isPresent())
        {
            logger.log(Level.WARNING, () -> "Failed to find matching quality version ID in retrieved quality versions for quality version name "
                    + qualityVersionNameFromSet + " or id " + qualityVersionIdFromSet
                    + ". Using NULL for quality version.");
        }
        return retVal;
    }

    private void logProgress(ProgressListener progressListener, Logger logFileLogger, String message, int percentComplete)
    {
        logFileLogger.info(() -> message);
        if(progressListener != null)
        {
            progressListener.progress(message, MessageType.IMPORTANT, percentComplete);
        }
    }

    private void logError(ProgressListener progressListener, Logger logFileLogger, String errorMsg, Throwable e)
    {
        if(progressListener != null)
        {
            progressListener.progress(errorMsg, MessageType.ERROR);
        }
        LOGGER.log(Level.CONFIG, e, () -> errorMsg);
        logFileLogger.log(Level.SEVERE, e, () -> errorMsg);
    }

}
