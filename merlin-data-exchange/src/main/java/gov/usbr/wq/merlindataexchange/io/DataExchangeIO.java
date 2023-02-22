package gov.usbr.wq.merlindataexchange.io;

import gov.usbr.wq.dataaccess.model.MeasureWrapper;
import gov.usbr.wq.merlindataexchange.DataExchangeCache;
import gov.usbr.wq.merlindataexchange.parameters.MerlinParameters;
import gov.usbr.wq.merlindataexchange.MerlinExchangeDaoCompletionTracker;
import gov.usbr.wq.merlindataexchange.configuration.DataExchangeSet;
import hec.ui.ProgressListener;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Logger;

public final class DataExchangeIO
{
    private DataExchangeIO()
    {
        throw new AssertionError("Utility class for reading and writing data. Don't instantiate");
    }

    public static CompletableFuture<Void> exchangeData(DataExchangeReader reader, DataExchangeWriter writer, DataExchangeSet dataExchangeSet, MerlinParameters runtimeParameters,
                                                       DataExchangeCache cache, MeasureWrapper measure, MerlinExchangeDaoCompletionTracker completionTracker, ProgressListener progressListener,
                                                       AtomicBoolean isCancelled, Logger logger, ExecutorService executorService)
    {
        CompletableFuture<Void> retVal = new CompletableFuture<>();
        if(!isCancelled.get())
        {
            logProgress(progressListener, logger, "Source path: " + reader.getSourcePath());
            logProgress(progressListener, logger, "Destination path: " + writer.getDestinationPath());
            retVal = reader.readData(dataExchangeSet, runtimeParameters, cache, measure,
                            completionTracker, progressListener, isCancelled, logger, executorService)
                    .thenAcceptAsync(tsc ->
                        writer.writeData(tsc, measure, runtimeParameters, completionTracker, progressListener, logger, isCancelled), executorService);


        }
        return retVal;
    }

    private static void logProgress(ProgressListener progressListener, Logger logger, String message)
    {
        if(progressListener != null)
        {
            progressListener.progress(message, ProgressListener.MessageType.IMPORTANT);
        }
        logger.info(() -> message);
    }

}
