package gov.usbr.wq.merlindataexchange.io;

import com.rma.io.DssFileManagerImpl;
import gov.usbr.wq.dataaccess.model.MeasureWrapper;
import gov.usbr.wq.merlindataexchange.MerlinDataExchangeLogBody;
import gov.usbr.wq.merlindataexchange.parameters.MerlinParameters;
import gov.usbr.wq.merlindataexchange.MerlinExchangeCompletionTracker;
import gov.usbr.wq.merlindataexchange.configuration.DataStore;
import hec.data.DataSetIllegalArgumentException;
import hec.heclib.dss.DSSPathname;
import hec.heclib.dss.HecTimeSeriesBase;
import hec.heclib.util.HecTime;
import hec.io.StoreOption;
import hec.io.TimeSeriesContainer;
import hec.ui.ProgressListener;
import hec.ui.ProgressListener.MessageType;
import rma.services.annotations.ServiceProvider;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

@ServiceProvider(service = DataExchangeWriter.class, position = 100, path = DataExchangeWriter.LOOKUP_PATH
        + "/" + DssDataExchangeWriter.DSS)
public final class DssDataExchangeWriter implements DataExchangeWriter
{
    public static final String DSS = "dss";
    private static final Logger LOGGER = Logger.getLogger(DssDataExchangeWriter.class.getName());
    private Path _dssWritePath;

    @Override
    public synchronized void writeData(TimeSeriesContainer timeSeriesContainer, MeasureWrapper measure, MerlinParameters runtimeParameters,
                                       MerlinExchangeCompletionTracker completionTracker, ProgressListener progressListener, MerlinDataExchangeLogBody logFileLogger, AtomicBoolean isCancelled)
    {
        StoreOption storeOption = runtimeParameters.getStoreOption();
        String seriesString = measure.getSeriesString();
        if(timeSeriesContainer != null && !isCancelled.get())
        {
            DSSPathname pathname = new DSSPathname(timeSeriesContainer.fullName);
            String progressMsg = "Read " + measure.getSeriesString() + " | Is processed: " + measure.isProcessed() + " | Events read: " + timeSeriesContainer.getNumberValues()
                    + ", expected " + getExpectedNumValues(runtimeParameters.getStart(), runtimeParameters.getEnd(), pathname.ePart(), ZoneId.of(timeSeriesContainer.getTimeZoneID()),
                    timeSeriesContainer.getStartTime(), timeSeriesContainer.getEndTime());
            logFileLogger.log(progressMsg);
            int percentComplete = completionTracker.readTaskCompleted();
            logProgress(progressListener, progressMsg, percentComplete);
            timeSeriesContainer.fileName = _dssWritePath.toString();
            int success = DssFileManagerImpl.getDssFileManager().writeTS(timeSeriesContainer, storeOption);
            if(success == 0)
            {
                String successMsg = "Write to " + timeSeriesContainer.fullName + " from " + seriesString;
                int percentCompleteAfterWrite = completionTracker.writeTaskCompleted();
                if(progressListener != null)
                {
                    progressListener.progress(successMsg, MessageType.GENERAL, percentCompleteAfterWrite);
                }
                logFileLogger.log(successMsg);
                LOGGER.config(() -> successMsg);
            }
            else
            {
                String failMsg = "Failed to write " +  seriesString + " to DSS! Error status code: " + success;
                if(progressListener != null)
                {
                    progressListener.progress(failMsg, MessageType.ERROR);
                }
                logFileLogger.log(failMsg);
                LOGGER.config(() -> failMsg);
            }
        }
    }

    static int getExpectedNumValues(Instant start, Instant end, String ePart, ZoneId tscZoneId, HecTime firstRealTime, HecTime lastRealTime)
    {
        int intervalMinutes = HecTimeSeriesBase.getIntervalFromEPart(ePart);
        long durationMinutes = Duration.between(start, end).toMinutes();
        boolean startIsBeforeFirstRealTime = start.isBefore(firstRealTime.getInstant(tscZoneId));
        boolean endIsAfterLastRealTime = end.isAfter(lastRealTime.getInstant(tscZoneId));
        int retVal = (int) (durationMinutes / ((double) intervalMinutes));
        if(!(startIsBeforeFirstRealTime && endIsAfterLastRealTime))
        {
            retVal ++;
        }
        return retVal;
    }

    @Override
    public void initialize(DataStore dataStore, MerlinParameters parameters)
    {
        _dssWritePath = buildAbsoluteDssWritePath(dataStore.getPath(), parameters.getWatershedDirectory());
    }

    @Override
    public void close()
    {
        DssFileManagerImpl.getDssFileManager().close(_dssWritePath.toString());
    }

    @Override
    public String getDestinationPath()
    {
        return _dssWritePath.toString();
    }

    private static Path buildAbsoluteDssWritePath(String filepath, Path watershedDir)
    {
        Path xmlFilePath = Paths.get(filepath);
        if(!xmlFilePath.isAbsolute() && filepath.contains("$WATERSHED"))
        {
            filepath = filepath.replace("$WATERSHED", watershedDir.toString());
            xmlFilePath = Paths.get(filepath);
        }
        return xmlFilePath;
    }

    private synchronized void logProgress(ProgressListener progressListener, String message, int percentComplete)
    {
        if(progressListener != null)
        {
            progressListener.progress(message, MessageType.GENERAL, percentComplete);
        }
    }

    private synchronized void logError(ProgressListener progressListener, MerlinDataExchangeLogBody logFileLogger, String errorMsg, DataSetIllegalArgumentException e)
    {
        if(progressListener != null)
        {
            progressListener.progress(errorMsg, MessageType.ERROR);
        }
        logFileLogger.log(errorMsg);
        LOGGER.log(Level.CONFIG, e, () -> errorMsg);
    }


}
