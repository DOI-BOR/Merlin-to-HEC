package gov.usbr.wq.merlindataexchange;

import gov.usbr.wq.dataaccess.MerlinTimeSeriesDataAccess;
import gov.usbr.wq.dataaccess.http.HttpAccessException;
import gov.usbr.wq.dataaccess.jwt.TokenContainer;
import gov.usbr.wq.dataaccess.model.DataWrapper;
import gov.usbr.wq.dataaccess.model.MeasureWrapper;
import gov.usbr.wq.merlintohec.exceptions.MerlinInvalidTimestepException;
import gov.usbr.wq.merlintohec.model.MerlinDaoConversionUtil;
import hec.io.TimeSeriesContainer;
import hec.ui.ProgressListener;
import hec.ui.ProgressListener.MessageType;

import java.io.IOException;
import java.time.Instant;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

final class MerlinDaoReader
{

    private static final Logger LOGGER = Logger.getLogger(MerlinDaoReader.class.getName());
    CompletableFuture<TimeSeriesContainer> readData(Instant startTime, Instant endTime, MeasureWrapper measure, Integer qualityVersionId,
                                                    TokenContainer accessToken, String unitSystemToConvertTo, String fPartOverride, MerlinExchangeDaoCompletionTracker completionTracker,
                                                    ProgressListener progressListener, AtomicBoolean isCancelled, ExecutorService executorService)
    {
        if (startTime == null)
        {
            startTime = Instant.ofEpochMilli(Long.MIN_VALUE);
        }
        if (endTime == null)
        {
            endTime =  Instant.ofEpochMilli(Long.MAX_VALUE);
        }

        Instant start = startTime;
        Instant end = endTime;
        progressListener.progress("Retrieving data for measure with series string: " + measure.getSeriesString() + "...", MessageType.IMPORTANT);
        return CompletableFuture.supplyAsync(() ->
        {
            DataWrapper data = retrieveDataWithUpdatedTimeWindow(start, end, measure, qualityVersionId,
                    accessToken, completionTracker, progressListener, isCancelled);
            TimeSeriesContainer retVal = null;
            if(!isCancelled.get())
            {
                try
                {
                    retVal = MerlinDaoConversionUtil.convertToTsc(data, unitSystemToConvertTo, fPartOverride, progressListener);
                }
                catch (MerlinInvalidTimestepException e)
                {
                    LOGGER.log(Level.WARNING, e, () -> "Unsupported timestep: " + data.getTimestep());
                    progressListener.progress("Skipping Measure with unsupported timestep", MessageType.IMPORTANT);
                }
            }
            return retVal;

        }, executorService);
    }

    private static DataWrapper retrieveDataWithUpdatedTimeWindow(Instant start, Instant end, MeasureWrapper measure, Integer qualityVersionId,
                                                                 TokenContainer accessToken, MerlinExchangeDaoCompletionTracker completionTracker,
                                                                 ProgressListener progressListener, AtomicBoolean isCancelled)
    {
        MerlinTimeSeriesDataAccess access = new MerlinTimeSeriesDataAccess();
        DataWrapper retVal = null;
        if(!isCancelled.get())
        {
            try
            {
                retVal = access.getEventsBySeries(accessToken, measure, qualityVersionId, start, end);
                progressListener.progress("Successfully retrieved data for " + measure.getSeriesString() + " with " + retVal.getEvents().size() + " events!", MessageType.IMPORTANT,
                        completionTracker.readWriteTaskCompleted());
            }
            catch (IOException | HttpAccessException ex)
            {
                LOGGER.log(Level.WARNING, ex, () -> "Failed to retrieve data for " + measure);
                progressListener.progress("Failed to retrieve data for measure with series string: " + measure.getSeriesString(), MessageType.ERROR);
            }
        }
        return retVal;
    }
}
