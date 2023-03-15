package gov.usbr.wq.merlindataexchange.io;

import gov.usbr.wq.dataaccess.model.MeasureWrapper;
import gov.usbr.wq.merlindataexchange.MerlinDataExchangeLogBody;
import gov.usbr.wq.merlindataexchange.configuration.DataStore;
import gov.usbr.wq.merlindataexchange.parameters.MerlinParameters;
import gov.usbr.wq.merlindataexchange.MerlinExchangeCompletionTracker;
import hec.io.TimeSeriesContainer;
import hec.ui.ProgressListener;

import java.util.concurrent.atomic.AtomicBoolean;

public interface DataExchangeWriter extends DataExchanger
{

    String LOOKUP_PATH = "dataexchange/reader";

    void writeData(TimeSeriesContainer timeSeriesContainer, MeasureWrapper seriesPath, MerlinParameters runtimeParameters, DataStore destinationDataStore, MerlinExchangeCompletionTracker completionTracker,
                   ProgressListener progressListener, MerlinDataExchangeLogBody logger, AtomicBoolean isCancelled);

    String getDestinationPath(DataStore destinationDataStore, MerlinParameters parameters);
}
