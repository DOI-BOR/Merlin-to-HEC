package gov.usbr.wq.merlindataexchange;

import com.rma.io.DssFileManager;
import hec.io.StoreOption;
import hec.io.TimeSeriesContainer;
import hec.ui.ProgressListener;

import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicBoolean;

final class MerlinDaoWriter
{
    private final DssFileManager _dssFileManager;
    private final Path _dssWritePath;

    public MerlinDaoWriter(DssFileManager dssFileManager, Path dssWritePath)
    {
        _dssFileManager = dssFileManager;
        _dssWritePath = dssWritePath;
    }

    void writeData(TimeSeriesContainer timeSeriesContainer, StoreOption storeOption, MerlinExchangeDaoCompletionTracker completionTracker,
                   ProgressListener progressListener, AtomicBoolean isCancelled)
    {
        int progressionIncrement = completionTracker.readWriteTaskCompleted();
        if(timeSeriesContainer != null && !isCancelled.get())
        {
            progressListener.progress("Successfully converted Measure to timeseries! Writing timeseries to " + _dssWritePath, ProgressListener.MessageType.IMPORTANT);
            timeSeriesContainer.fileName = _dssWritePath.toString();
            int success = _dssFileManager.writeTS(timeSeriesContainer, storeOption);
            if(success == 0)
            {
                progressListener.progress("Measure successfully written to DSS!", ProgressListener.MessageType.IMPORTANT, progressionIncrement);
            }
            else
            {
                progressListener.progress("Failed to write Measure to DSS! Error status code: " + success, ProgressListener.MessageType.ERROR, progressionIncrement);
            }
        }
    }

    void close()
    {
        _dssFileManager.close(_dssWritePath.toString());
    }
}
