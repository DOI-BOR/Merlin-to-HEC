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
import gov.usbr.wq.merlindataexchange.MerlinDataExchangeLogBody;
import gov.usbr.wq.merlindataexchange.NoEventsException;
import gov.usbr.wq.merlindataexchange.parameters.MerlinParameters;
import gov.usbr.wq.merlindataexchange.MerlinExchangeCompletionTracker;
import gov.usbr.wq.merlindataexchange.parameters.UsernamePasswordHolder;
import gov.usbr.wq.merlindataexchange.parameters.UsernamePasswordNotFoundException;
import gov.usbr.wq.merlindataexchange.configuration.DataExchangeSet;
import gov.usbr.wq.merlindataexchange.configuration.DataStore;
import hec.heclib.dss.HecTimeSeriesBase;
import hec.heclib.util.HecTime;
import hec.io.TimeSeriesContainer;
import hec.ui.ProgressListener;
import hec.ui.ProgressListener.MessageType;
import rma.services.annotations.ServiceProvider;

import java.io.IOException;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
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
                                                           MerlinExchangeCompletionTracker completionTracker, ProgressListener progressListener, AtomicBoolean isCancelled,
                                                           MerlinDataExchangeLogBody logFileLogger, ExecutorService executorService)
    {
        Instant start = runtimeParameters.getStart();
        Instant end = runtimeParameters.getEnd();
        String fPartOverride = runtimeParameters.getFPartOverride();
        QualityVersionWrapper qualityVersion = getQualityVersionIdFromDataExchangeSet(dataExchangeSet, cache).orElse(null);
        String unitSystemToConvertTo = dataExchangeSet.getUnitSystem();
        Integer qualityVersionId = qualityVersion == null ? null : qualityVersion.getQualityVersionID();
        return CompletableFuture.supplyAsync(() ->
        {
            TimeSeriesContainer retVal = null;
            try
            {
                UsernamePasswordHolder usernamePassword = runtimeParameters.getUsernamePasswordForUrl(_merlinApiRoot);
                retVal = retrieveDataAsTimeSeries(usernamePassword, start, end, measure, qualityVersionId, progressListener, logFileLogger,
                        isCancelled, fPartOverride, unitSystemToConvertTo, completionTracker, measure.isProcessed());
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
                                                         ProgressListener progressListener, MerlinDataExchangeLogBody logFileLogger,
                                                         AtomicBoolean isCancelled, String fPartOverride, String unitSystemToConvertTo, MerlinExchangeCompletionTracker completionTracker,
                                                         Boolean isProcessed)
    {
        TimeSeriesContainer retVal = null;
        try
        {
            _token = generateNewTokenIfNecessary(new ApiConnectionInfo(_merlinApiRoot), usernamePassword.getUsername(), usernamePassword.getPassword());
            DataWrapper data = retrieveData(start, end, measure, qualityVersionId, progressListener, logFileLogger, isCancelled);
            if(data == null)
            {
                String errorMsg = "Failed to retrieve data for measure: " + measure.getSeriesString();
                if(progressListener != null)
                {
                    progressListener.progress(errorMsg, MessageType.ERROR);
                }
                logFileLogger.log(errorMsg);
                LOGGER.config(() -> errorMsg);
            }
            else if(!isCancelled.get())
            {
                retVal = convertToTsc(data, unitSystemToConvertTo, fPartOverride, progressListener, logFileLogger, completionTracker, isProcessed, start, end);
            }
        }
        catch (HttpAccessException e)
        {
            String errorMsg = "Failed to authenticate user: " + usernamePassword.getUsername();
            logError(progressListener, logFileLogger, errorMsg, e);
        }
        return retVal;
    }

    private TimeSeriesContainer convertToTsc(DataWrapper data, String unitSystemToConvertTo, String fPartOverride, ProgressListener progressListener, MerlinDataExchangeLogBody logFileLogger,
                                             MerlinExchangeCompletionTracker completionTracker, Boolean isProcessed, Instant start, Instant end)
    {
        TimeSeriesContainer retVal = null;
        try
        {
            retVal =  MerlinDataConverter.dataToTimeSeries(data, unitSystemToConvertTo, fPartOverride, progressListener);
        }
        catch (MerlinInvalidTimestepException e)
        {
            String msg = "Skipping " + data.getSeriesId() + " with unsupported timestep: " + data.getTimestep() + " | Is processed: " + isProcessed;
            logFileLogger.log(msg);
            logProgressMessage(progressListener, msg);
            LOGGER.log(Level.CONFIG, e, () -> "Unsupported timestep: " + data.getTimestep());
        }
        catch (NoEventsException e)
        {
            if(start == null)
            {
                start = data.getStartTime().toInstant();
            }
            if(end == null)
            {
                end = data.getEndTime().toInstant();
            }
            Instant startDetermined = start;
            Instant endDetermined = end;
            ZoneId zoneId = data.getTimeZone();
            int expectedNumValues = ExpectedNumberValuesCalculator.getExpectedNumValues(start, end,
                    HecTimeSeriesBase.getEPartFromInterval(Integer.parseInt(data.getTimestep())), zoneId,
                    HecTime.fromZonedDateTime(ZonedDateTime.ofInstant(start, zoneId)), HecTime.fromZonedDateTime(ZonedDateTime.ofInstant(end, zoneId)));
            String progressMsg = "Read " + data.getSeriesId() + " | Is processed: " + isProcessed + " | Values read: 0"
                    + ", 0 missing, " + expectedNumValues + " expected";
            int readPercentIncrement = completionTracker.readTaskCompleted();
            logFileLogger.log(progressMsg);
            logProgressMessage(progressListener, progressMsg, readPercentIncrement);
            int nothingToWritePercentIncrement = completionTracker.writeTaskCompleted();
            logFileLogger.log(e.getMessage());
            logProgressMessage(progressListener, e.getMessage(), nothingToWritePercentIncrement);
            LOGGER.log(Level.CONFIG, e, () -> "No events for " + data.getSeriesId() + " in time window " + startDetermined + " | " + endDetermined);
        }
        return retVal;
    }

    private synchronized void logProgressMessage(ProgressListener progressListener, String message, int nothingToWritePercentIncrement)
    {
        if (progressListener != null)
        {
            progressListener.progress(message, MessageType.GENERAL, nothingToWritePercentIncrement);
        }
    }

    private synchronized void logProgressMessage(ProgressListener progressListener, String msg)
    {
        if (progressListener != null)
        {
            progressListener.progress(msg, MessageType.GENERAL);
        }
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
                                     ProgressListener progressListener, MerlinDataExchangeLogBody logFileLogger, AtomicBoolean isCancelled)
    {
        MerlinTimeSeriesDataAccess access = new MerlinTimeSeriesDataAccess();
        DataWrapper retVal = null;
        if(!isCancelled.get())
        {
            try
            {
                retVal = access.getEventsBySeries(new ApiConnectionInfo(_merlinApiRoot), _token, measure, qualityVersionId, start, end);
            }
            catch (IOException | HttpAccessException ex)
            {
                logError(progressListener, logFileLogger, "Failed to retrieve data for measure with series string: " + measure.getSeriesString(), ex);
            }
        }
        return retVal;
    }

    private Optional<QualityVersionWrapper> getQualityVersionIdFromDataExchangeSet(DataExchangeSet dataExchangeSet, DataExchangeCache cache)
    {
        String qualityVersionNameFromSet = dataExchangeSet.getQualityVersionName();
        Integer qualityVersionIdFromSet = dataExchangeSet.getQualityVersionId();
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
            LOGGER.log(Level.WARNING, () -> "Failed to find matching quality version ID in retrieved quality versions for quality version name "
                    + qualityVersionNameFromSet + " or id " + qualityVersionIdFromSet
                    + ". Using NULL for quality version.");
        }
        return retVal;
    }

    private synchronized void logError(ProgressListener progressListener, MerlinDataExchangeLogBody logFileLogger, String errorMsg, Throwable e)
    {
        if(progressListener != null)
        {
            progressListener.progress(errorMsg, MessageType.ERROR);
        }
        LOGGER.log(Level.CONFIG, e, () -> errorMsg);
        logFileLogger.log("Error occurred: " + errorMsg);
    }

}
