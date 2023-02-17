package gov.usbr.wq.merlindataexchange.io;

import gov.usbr.wq.merlindataexchange.MerlinDataExchangeParameters;
import gov.usbr.wq.merlindataexchange.MerlinExchangeDaoCompletionTracker;
import hec.io.TimeSeriesContainer;
import hec.ui.ProgressListener;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Logger;

public interface DataExchangeWriter extends DataExchanger
{

    String LOOKUP_PATH = "dataexchange/reader";

    void writeData(TimeSeriesContainer timeSeriesContainer, String seriesPath, MerlinDataExchangeParameters runtimeParameters, MerlinExchangeDaoCompletionTracker completionTracker,
                   ProgressListener progressListener, Logger logger, AtomicBoolean isCancelled);

    String getDestinationPath();
}
