package gov.usbr.wq.merlindataexchange.io;

import gov.usbr.wq.dataaccess.model.MeasureWrapper;
import gov.usbr.wq.merlindataexchange.DataExchangeCache;
import gov.usbr.wq.merlindataexchange.MerlinDataExchangeLogBody;
import gov.usbr.wq.merlindataexchange.configuration.DataStore;
import gov.usbr.wq.merlindataexchange.parameters.MerlinParameters;
import gov.usbr.wq.merlindataexchange.MerlinExchangeCompletionTracker;
import gov.usbr.wq.merlindataexchange.configuration.DataExchangeSet;
import hec.ui.ProgressListener;

import java.time.Instant;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

public final class DataExchangeIO
{
    private DataExchangeIO()
    {
        throw new AssertionError("Utility class for reading and writing data. Don't instantiate");
    }

    @SuppressWarnings("unchecked")
    public static <T> CompletableFuture<Void> exchangeData(DataExchangeReader<?> reader, DataExchangeWriter<T> writer, DataExchangeSet dataExchangeSet, MerlinParameters runtimeParameters,
                                                       DataStore source, DataStore destination, DataExchangeCache cache, MeasureWrapper measure, MerlinExchangeCompletionTracker completionTracker,
                                                       ProgressListener progressListener, AtomicBoolean isCancelled, MerlinDataExchangeLogBody logger, ExecutorService executorService)
    {
        Instant readStart = Instant.now();
        CompletableFuture<Void> retVal = new CompletableFuture<>();
        if(!isCancelled.get())
        {
            AtomicReference<String> readDurationString = new AtomicReference<>("");
            retVal = reader.readData(dataExchangeSet, runtimeParameters, source, destination, cache, measure,
                            completionTracker, progressListener, isCancelled, logger, executorService, readDurationString)
                    .thenAcceptAsync(objectRead ->
                    {
                        writer.writeData((T) objectRead, measure, dataExchangeSet, runtimeParameters, cache, destination, completionTracker, progressListener, logger, isCancelled, readDurationString);
                        Instant writeEnd = Instant.now();
                        String totalDuration = ReadWriteTimestampUtil.getDuration(readStart, writeEnd);
                        if(!totalDuration.isEmpty())
                        {
                            String msg = "Processed " + measure.getSeriesString() + totalDuration;
                            logger.log(msg);
                            if(progressListener != null)
                            {
                                progressListener.progress(msg, ProgressListener.MessageType.GENERAL);
                            }
                        }
                    }, executorService);



        }
        return retVal;
    }
}
