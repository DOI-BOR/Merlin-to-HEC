package gov.usbr.wq.merlindataexchange.io;

import gov.usbr.wq.dataaccess.http.ApiConnectionInfo;
import gov.usbr.wq.dataaccess.http.HttpAccessException;
import gov.usbr.wq.dataaccess.http.TokenContainer;
import gov.usbr.wq.dataaccess.model.MeasureWrapper;
import gov.usbr.wq.dataaccess.model.QualityVersionWrapper;
import gov.usbr.wq.merlindataexchange.DataExchangeCache;
import gov.usbr.wq.merlindataexchange.MerlinDataExchangeLogBody;
import gov.usbr.wq.merlindataexchange.MerlinExchangeCompletionTracker;
import gov.usbr.wq.merlindataexchange.configuration.DataExchangeSet;
import gov.usbr.wq.merlindataexchange.configuration.DataStore;
import gov.usbr.wq.merlindataexchange.parameters.MerlinParameters;
import gov.usbr.wq.merlindataexchange.parameters.UsernamePasswordHolder;
import gov.usbr.wq.merlindataexchange.parameters.UsernamePasswordNotFoundException;
import hec.ui.ProgressListener;

import java.time.Instant;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;
import java.util.logging.Logger;

public abstract class MerlinDataExchangeReader<P extends MerlinParameters, S, T> implements DataExchangeReader<P, T>
{
    public static final String MERLIN = "merlin";
    private static final Logger LOGGER = Logger.getLogger(MerlinDataExchangeReader.class.getName());
    @Override
    public CompletableFuture<T> readData(DataExchangeSet dataExchangeSet, P runtimeParameters, DataStore sourceDataStore, DataStore destDataStore, DataExchangeCache cache,
                                         MeasureWrapper measure, MerlinExchangeCompletionTracker completionTracker, ProgressListener progressListener, AtomicBoolean isCancelled,
                                         MerlinDataExchangeLogBody logFileLogger, ExecutorService executorService, AtomicReference<String> readDurationString)
    {
        String merlinApiRoot = getSourcePath(sourceDataStore, runtimeParameters);
        Instant start = runtimeParameters.getStart();
        Instant end = runtimeParameters.getEnd();
        QualityVersionWrapper qualityVersion = QualityVersionFromSetUtil.getQualityVersionIdFromDataExchangeSet(dataExchangeSet, cache).orElse(null);
        String unitSystemToConvertTo = dataExchangeSet.getUnitSystem();
        Integer qualityVersionId = qualityVersion == null ? null : qualityVersion.getQualityVersionID();
        return CompletableFuture.supplyAsync(() ->
        {
            T retVal = null;
            try
            {
                UsernamePasswordHolder usernamePassword = runtimeParameters.getUsernamePasswordForUrl(merlinApiRoot);
                retVal = retrieveDataAsType(usernamePassword, start, end, destDataStore, dataExchangeSet, cache, merlinApiRoot, measure, qualityVersionId, progressListener, logFileLogger,
                        isCancelled, runtimeParameters, unitSystemToConvertTo, completionTracker, measure.isProcessed(), readDurationString);
            }
            catch (UsernamePasswordNotFoundException e)
            {
                String errorMsg = "Failed to find username/password in parameters for URL: " + merlinApiRoot;
                logError(progressListener, logFileLogger, errorMsg, e);
            }
            return retVal;

        }, executorService);
    }

    protected T retrieveDataAsType(UsernamePasswordHolder usernamePassword, Instant start, Instant end, DataStore dataStore, DataExchangeSet dataExchangeSet,
                                   DataExchangeCache cache, String merlinApiRoot, MeasureWrapper measure, Integer qualityVersionId, ProgressListener progressListener,
                                   MerlinDataExchangeLogBody logFileLogger, AtomicBoolean isCancelled, P runTimeParameters, String unitSystemToConvertTo,
                                   MerlinExchangeCompletionTracker completionTracker, Boolean isProcessed, AtomicReference<String> readDurationString)
    {
        T retVal = null;
        try
        {
            TokenRegistry tokenRegistry = TokenRegistry.getRegistry();
            TokenContainer token = tokenRegistry.getToken(new ApiConnectionInfo(merlinApiRoot), usernamePassword.getUsername(), usernamePassword.getPassword());
            Instant readStart = Instant.now();
            S data = retrieveData(start, end, dataExchangeSet, cache, merlinApiRoot, token, measure, qualityVersionId, dataStore, progressListener, logFileLogger, isCancelled);
            Instant readEnd = Instant.now();
            readDurationString.set(ReadWriteTimestampUtil.getDuration(readStart, readEnd));
            if(data == null)
            {
                String errorMsg = "Failed to retrieve data for measure: " + measure.getSeriesString();
                if(progressListener != null)
                {
                    progressListener.progress(errorMsg, ProgressListener.MessageType.ERROR);
                }
                logFileLogger.log(errorMsg);
                LOGGER.config(() -> errorMsg);
            }
            else if(!isCancelled.get())
            {
                retVal = convertToType(data, dataStore, unitSystemToConvertTo, runTimeParameters, progressListener, logFileLogger, completionTracker, isProcessed, start, end, readDurationString, measure);
            }
        }
        catch (HttpAccessException e)
        {
            String errorMsg = "Failed to authenticate user: " + usernamePassword.getUsername();
            logError(progressListener, logFileLogger, errorMsg, e);
        }
        return retVal;
    }

    protected abstract T convertToType(S data, DataStore sourceDataStore, String unitSystemToConvertTo, P parameters, ProgressListener progressListener,
                                       MerlinDataExchangeLogBody logFileLogger, MerlinExchangeCompletionTracker completionTracker, Boolean isProcessed,
                                       Instant start, Instant end, AtomicReference<String> readDurationString, MeasureWrapper measure);

    protected abstract S retrieveData(Instant start, Instant end, DataExchangeSet dataExchangeSet, DataExchangeCache cache, String merlinApiRoot, TokenContainer token,
                                      MeasureWrapper measure, Integer qualityVersionId, DataStore sourceDataStore, ProgressListener progressListener,
                                      MerlinDataExchangeLogBody logFileLogger, AtomicBoolean isCancelled);

    @Override
    public String getSourcePath(DataStore sourceDataStore, MerlinParameters parameters)
    {
        return sourceDataStore.getPath();
    }

    protected void logProgressMessage(ProgressListener progressListener, String message, int nothingToWritePercentIncrement)
    {
        if (progressListener != null)
        {
            progressListener.progress(message, ProgressListener.MessageType.GENERAL, nothingToWritePercentIncrement);
        }
    }

    void logProgressMessage(ProgressListener progressListener, String msg)
    {
        if (progressListener != null)
        {
            progressListener.progress(msg, ProgressListener.MessageType.GENERAL);
        }
    }

    protected void logError(ProgressListener progressListener, MerlinDataExchangeLogBody logFileLogger, String errorMsg, Throwable e)
    {
        if(progressListener != null)
        {
            progressListener.progress(errorMsg, ProgressListener.MessageType.ERROR);
        }
        LOGGER.log(Level.CONFIG, e, () -> errorMsg);
        logFileLogger.log("Error occurred: " + errorMsg);
    }
}
