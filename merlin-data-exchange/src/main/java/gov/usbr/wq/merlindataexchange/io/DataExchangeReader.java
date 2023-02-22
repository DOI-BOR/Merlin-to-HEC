package gov.usbr.wq.merlindataexchange.io;

import gov.usbr.wq.dataaccess.model.MeasureWrapper;
import gov.usbr.wq.merlindataexchange.DataExchangeCache;
import gov.usbr.wq.merlindataexchange.parameters.MerlinParameters;
import gov.usbr.wq.merlindataexchange.MerlinExchangeDaoCompletionTracker;
import gov.usbr.wq.merlindataexchange.configuration.DataExchangeSet;
import hec.io.TimeSeriesContainer;
import hec.ui.ProgressListener;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Logger;

public interface DataExchangeReader extends DataExchanger
{
    String LOOKUP_PATH = "dataexchange/reader";
    CompletableFuture<TimeSeriesContainer> readData(DataExchangeSet configuration, MerlinParameters runtimeParameters, DataExchangeCache cache, MeasureWrapper seriesPath,
                                                    MerlinExchangeDaoCompletionTracker completionTracker,
                                                    ProgressListener progressListener, AtomicBoolean isCancelled, Logger logger, ExecutorService executorService);

    String getSourcePath();
}
