package gov.usbr.wq.merlindataexchange.io;

import gov.usbr.wq.dataaccess.model.MeasureWrapper;
import gov.usbr.wq.merlindataexchange.DataExchangeCache;
import gov.usbr.wq.merlindataexchange.MerlinDataExchangeLogBody;
import gov.usbr.wq.merlindataexchange.configuration.DataExchangeSet;
import gov.usbr.wq.merlindataexchange.configuration.DataStore;
import gov.usbr.wq.merlindataexchange.parameters.MerlinParameters;
import gov.usbr.wq.merlindataexchange.MerlinExchangeCompletionTracker;
import hec.ui.ProgressListener;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

public interface DataExchangeWriter<T> extends DataExchanger
{

    String LOOKUP_PATH = "dataexchange/reader";

    void writeData(T dataObject, MeasureWrapper seriesPath, DataExchangeSet set, MerlinParameters runtimeParameters, DataExchangeCache cache, DataStore destinationDataStore,
                   MerlinExchangeCompletionTracker completionTracker, ProgressListener progressListener, MerlinDataExchangeLogBody logger,
                   AtomicBoolean isCancelled, AtomicReference<String> readStart);

    default String getDestinationPath(DataStore destinationDataStore, MerlinParameters parameters)
    {
        String filepath = destinationDataStore.getPath();
        Path watershedDir = parameters.getWatershedDirectory();
        Path xmlFilePath = Paths.get(filepath);
        if(!xmlFilePath.isAbsolute() && filepath.contains("$WATERSHED"))
        {
            filepath = filepath.replace("$WATERSHED", watershedDir.toString());
            xmlFilePath = Paths.get(filepath);
        }
        return xmlFilePath.toString();
    }

}
