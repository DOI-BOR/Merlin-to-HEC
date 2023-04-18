package gov.usbr.wq.merlindataexchange.io;

import gov.usbr.wq.dataaccess.model.MeasureWrapper;
import gov.usbr.wq.merlindataexchange.DataExchangeCache;
import gov.usbr.wq.merlindataexchange.MerlinDataExchangeLogBody;
import gov.usbr.wq.merlindataexchange.configuration.DataStore;
import gov.usbr.wq.merlindataexchange.parameters.MerlinParameters;
import gov.usbr.wq.merlindataexchange.MerlinExchangeCompletionTracker;
import gov.usbr.wq.merlindataexchange.configuration.DataExchangeSet;
import hec.ui.ProgressListener;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

public interface DataExchangeReader<T> extends DataExchanger
{
    String LOOKUP_PATH = "dataexchange/reader";
    CompletableFuture<T> readData(DataExchangeSet configuration, MerlinParameters runtimeParameters, DataStore sourceDataStore, DataStore destDataStore,
                                  DataExchangeCache cache, MeasureWrapper seriesPath, MerlinExchangeCompletionTracker completionTracker, ProgressListener progressListener,
                                  AtomicBoolean isCancelled, MerlinDataExchangeLogBody logger, ExecutorService executorService,
                                  AtomicReference<String> readStart);
    String getSourcePath(DataStore sourceDataStore, MerlinParameters parameters);

    List<MeasureWrapper> filterMeasuresToRead(DataExchangeSet dataExchangeSet, List<MeasureWrapper> measures);
}
