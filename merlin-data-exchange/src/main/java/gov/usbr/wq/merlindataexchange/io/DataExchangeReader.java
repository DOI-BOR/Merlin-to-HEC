package gov.usbr.wq.merlindataexchange.io;

import gov.usbr.wq.dataaccess.model.MeasureWrapper;
import gov.usbr.wq.merlindataexchange.DataExchangeCache;
import gov.usbr.wq.merlindataexchange.MerlinDataExchangeLogBody;
import gov.usbr.wq.merlindataexchange.configuration.DataStore;
import gov.usbr.wq.merlindataexchange.parameters.MerlinParameters;
import gov.usbr.wq.merlindataexchange.MerlinExchangeCompletionTracker;
import gov.usbr.wq.merlindataexchange.configuration.DataExchangeSet;
import hec.io.TimeSeriesContainer;
import hec.ui.ProgressListener;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;

public interface DataExchangeReader extends DataExchanger
{
    String LOOKUP_PATH = "dataexchange/reader";
    CompletableFuture<TimeSeriesContainer> readData(DataExchangeSet configuration, MerlinParameters runtimeParameters, DataStore sourceDataStore, DataExchangeCache cache, MeasureWrapper seriesPath,
                                                    MerlinExchangeCompletionTracker completionTracker,
                                                    ProgressListener progressListener, AtomicBoolean isCancelled, MerlinDataExchangeLogBody logger, ExecutorService executorService);
    String getSourcePath(DataStore sourceDataStore, MerlinParameters parameters);
}
