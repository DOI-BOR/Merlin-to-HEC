package gov.usbr.wq.merlindataexchange.io;

import gov.usbr.wq.dataaccess.model.MeasureWrapper;
import gov.usbr.wq.merlindataexchange.DataExchangeCache;
import gov.usbr.wq.merlindataexchange.MerlinDataExchangeLogBody;
import gov.usbr.wq.merlindataexchange.configuration.DataStore;
import gov.usbr.wq.merlindataexchange.parameters.MerlinParameters;
import gov.usbr.wq.merlindataexchange.MerlinExchangeCompletionTracker;
import gov.usbr.wq.merlindataexchange.configuration.DataExchangeSet;
import hec.ui.ProgressListener;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;

public final class DataExchangeIO
{
    private DataExchangeIO()
    {
        throw new AssertionError("Utility class for reading and writing data. Don't instantiate");
    }

    public static CompletableFuture<Void> exchangeData(DataExchangeReader reader, DataExchangeWriter writer, DataExchangeSet dataExchangeSet, MerlinParameters runtimeParameters,
                                                       DataStore source, DataStore destination, DataExchangeCache cache, MeasureWrapper measure, MerlinExchangeCompletionTracker completionTracker,
                                                       ProgressListener progressListener, AtomicBoolean isCancelled, MerlinDataExchangeLogBody logger, ExecutorService executorService)
    {
        CompletableFuture<Void> retVal = new CompletableFuture<>();
        if(!isCancelled.get())
        {
            retVal = reader.readData(dataExchangeSet, runtimeParameters, source, cache, measure,
                            completionTracker, progressListener, isCancelled, logger, executorService)
                    .thenAcceptAsync(tsc ->
                        writer.writeData(tsc, measure, runtimeParameters, destination, completionTracker, progressListener, logger, isCancelled), executorService);


        }
        return retVal;
    }
}
