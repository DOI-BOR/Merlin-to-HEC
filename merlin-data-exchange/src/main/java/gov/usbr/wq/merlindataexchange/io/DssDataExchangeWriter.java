package gov.usbr.wq.merlindataexchange.io;

import com.rma.io.DssFileManagerImpl;
import gov.usbr.wq.dataaccess.model.MeasureWrapper;
import gov.usbr.wq.merlindataexchange.MerlinDataExchangeLogBody;
import gov.usbr.wq.merlindataexchange.parameters.MerlinParameters;
import gov.usbr.wq.merlindataexchange.MerlinExchangeCompletionTracker;
import gov.usbr.wq.merlindataexchange.configuration.DataStore;
import hec.data.DataSetIllegalArgumentException;
import hec.heclib.dss.DSSPathname;
import hec.io.StoreOption;
import hec.io.TimeSeriesContainer;
import hec.lang.Const;
import hec.ui.ProgressListener;
import hec.ui.ProgressListener.MessageType;
import rma.services.annotations.ServiceProvider;

import java.nio.file.Path;
import java.nio.file.Paths;
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
    private static final String MERLIN_TO_DSS_WRITE_SINGLE_THREAD_PROPERTY_KEY = "merlin.dataexchange.writer.dss.singlethread";

    @Override
    public void writeData(TimeSeriesContainer timeSeriesContainer, MeasureWrapper measure, MerlinParameters runtimeParameters, DataStore destinationDataStore,
                                       MerlinExchangeCompletionTracker completionTracker, ProgressListener progressListener, MerlinDataExchangeLogBody logFileLogger, AtomicBoolean isCancelled)
    {
        Path dssWritePath = Paths.get(getDestinationPath(destinationDataStore, runtimeParameters));
        String seriesString = measure.getSeriesString();
        if(timeSeriesContainer != null && !isCancelled.get())
        {
            timeSeriesContainer.fileName = dssWritePath.toString();
            String useSingleThreadString = System.getProperty(MERLIN_TO_DSS_WRITE_SINGLE_THREAD_PROPERTY_KEY);
            boolean useSingleThreading = false;
            if(useSingleThreadString != null)
            {
                LOGGER.log(Level.FINE, () -> "Merlin to dss write with single thread using System Property " + MERLIN_TO_DSS_WRITE_SINGLE_THREAD_PROPERTY_KEY + " set to: " + useSingleThreadString);
                useSingleThreading = Boolean.parseBoolean(useSingleThreadString);
            }
            int success;
            if(useSingleThreading)
            {
                try(CloseableReentrantLock lock = ReadWriteLockManager.getInstance().getCloseableLock().lockIt())
                {
                    success = writeDss(timeSeriesContainer, runtimeParameters, measure, completionTracker, logFileLogger, progressListener);
                }
            }
            else
            {
                success = writeDss(timeSeriesContainer, runtimeParameters, measure, completionTracker, logFileLogger, progressListener);
            }
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
            DssFileManagerImpl.getDssFileManager().close(dssWritePath.toString());
        }
    }

    private int writeDss(TimeSeriesContainer timeSeriesContainer, MerlinParameters runtimeParameters, MeasureWrapper measure,
                         MerlinExchangeCompletionTracker completionTracker, MerlinDataExchangeLogBody logFileLogger, ProgressListener progressListener)
    {
        int success;
        StoreOption storeOption = runtimeParameters.getStoreOption();
        DSSPathname pathname = new DSSPathname(timeSeriesContainer.fullName);
        int numTrimmedValues = getNumTrimmedValues(timeSeriesContainer);
        int numExpected = ExpectedNumberValuesCalculator.getExpectedNumValues(runtimeParameters.getStart(), runtimeParameters.getEnd(), pathname.ePart(),
                ZoneId.of(timeSeriesContainer.getTimeZoneID()), timeSeriesContainer.getStartTime(), timeSeriesContainer.getEndTime());
        String progressMsg = "Read " + measure.getSeriesString() + " | Is processed: " + measure.isProcessed() + " | Values read: " + timeSeriesContainer.getNumberValues()
                + ", " + numTrimmedValues + " missing, " +  numExpected + " expected" ;
        logFileLogger.log(progressMsg);
        int percentComplete = completionTracker.readTaskCompleted();
        logProgress(progressListener, progressMsg, percentComplete);

        //write(timeseriesContainer) uses store option zero, so this ensures correct functionality for regular store flag 0
        //writeTS has a bug in its current state that can cause dss to write to wrong file. Once fixed, this conditional check won't be needed
        if(runtimeParameters.getStoreOption().getRegular() == 0)
        {
            success = DssFileManagerImpl.getDssFileManager().write(timeSeriesContainer);
        }
        else
        {
            success = DssFileManagerImpl.getDssFileManager().writeTS(timeSeriesContainer, storeOption);
        }
        return success;
    }

    @Override
    public String getDestinationPath(DataStore destinationDataStore, MerlinParameters parameters)
    {
        return buildAbsoluteDssWritePath(destinationDataStore.getPath(), parameters.getWatershedDirectory()).toString();
    }

    private int getNumTrimmedValues(TimeSeriesContainer timeSeriesContainer)
    {
        int missingCount = 0;
        for(double val : timeSeriesContainer.getValues())
        {
            if(val == Const.UNDEFINED_DOUBLE)
            {
                missingCount ++;
            }
        }
        return missingCount;
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
