package gov.usbr.wq.merlindataexchange.io.wq;

import gov.usbr.wq.dataaccess.model.MeasureWrapper;
import gov.usbr.wq.merlindataexchange.MerlinDataExchangeLogBody;
import gov.usbr.wq.merlindataexchange.MerlinExchangeCompletionTracker;
import gov.usbr.wq.merlindataexchange.configuration.DataStore;
import gov.usbr.wq.merlindataexchange.io.CloseableReentrantLock;
import gov.usbr.wq.merlindataexchange.io.DataExchangeWriter;
import gov.usbr.wq.merlindataexchange.io.ReadWriteLockManager;
import gov.usbr.wq.merlindataexchange.io.ReadWriteTimestampUtil;
import gov.usbr.wq.merlindataexchange.parameters.MerlinParameters;
import hec.ui.ProgressListener;
import rma.services.annotations.ServiceProvider;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;
import java.util.logging.Logger;

@ServiceProvider(service = DataExchangeWriter.class, position = 200, path = DataExchangeWriter.LOOKUP_PATH
        + "/" + CsvProfileWriter.CSV)
public final class CsvProfileWriter implements DataExchangeWriter<List<ProfileSample>>
{
    private static final Logger LOGGER = Logger.getLogger(CsvProfileWriter.class.getName());
    public static final String CSV = "csv";
    public static final String MERLIN_TO_CSV_PROFILE_WRITE_SINGLE_THREAD_PROPERTY_KEY = "merlin.dataexchange.writer.csv.profile.singlethread";
    private final AtomicBoolean _loggedThreadProperty = new AtomicBoolean(false);

    @Override
    public void writeData(List<ProfileSample> profileSamples, MeasureWrapper measure, MerlinParameters runtimeParameters,
                          DataStore destinationDataStore, MerlinExchangeCompletionTracker completionTracker, ProgressListener progressListener,
                          MerlinDataExchangeLogBody logFileLogger, AtomicBoolean isCancelled, AtomicReference<String> readDurationString, AtomicReference<List<String>> seriesIds)
    {
        if(profileSamples == null)
        {
            return;
        }
        Path csvWritePath = Paths.get(getDestinationPath(destinationDataStore, runtimeParameters));
        boolean useSingleThreading = isSingleThreaded();
        Instant writeStart;
        Instant writeEnd;
        if(useSingleThreading)
        {
            try(CloseableReentrantLock lock = ReadWriteLockManager.getInstance().getCloseableLock().lockIt())
            {
                writeStart = Instant.now();
                writeCsv(profileSamples, csvWritePath, measure, completionTracker, logFileLogger, progressListener, readDurationString, seriesIds);
                writeEnd = Instant.now();
            }
        }
        else
        {
            writeStart = Instant.now();
            writeCsv(profileSamples, csvWritePath, measure, completionTracker, logFileLogger, progressListener, readDurationString, seriesIds);
            writeEnd = Instant.now();
        }
        String successMsg = "Write to " + csvWritePath + " from " + measure.getSeriesString() + ReadWriteTimestampUtil.getDuration(writeStart, writeEnd);
        //two write tasks
        completionTracker.readWriteTaskCompleted();
        int percentCompleteAfterWrite = completionTracker.readWriteTaskCompleted();
        completionTracker.writeTaskCompleted();
        if(progressListener != null)
        {
            progressListener.progress(successMsg, ProgressListener.MessageType.GENERAL, percentCompleteAfterWrite);
        }
        logFileLogger.log(successMsg);
        LOGGER.config(() -> successMsg);

    }

    private void writeCsv(List<ProfileSample> profileSamples, Path csvWritePath, MeasureWrapper measure, MerlinExchangeCompletionTracker completionTracker,
                          MerlinDataExchangeLogBody logFileLogger, ProgressListener progressListener, AtomicReference<String> readDurationString, AtomicReference<List<String>> seriesIds)
    {
        List<String> seriesIdList = seriesIds.get();
        String seriesIdsString = String.join(",\n", seriesIdList);
        try
        {
            int totalSize = profileSamples.stream()
                    .mapToInt(sample -> sample.getConstituentDataList().stream()
                        .mapToInt(cdl -> cdl.getDataValues().size())
                        .sum())
                    .sum();
            String progressMsg = "Read " + seriesIdsString + " | Is processed: "
                    + measure.isProcessed() + " | Values read: " + totalSize + readDurationString;
            logFileLogger.log(progressMsg);
            for(int i=1; i < profileSamples.get(0).getConstituentDataList().size(); i++)
            {
                completionTracker.readWriteTaskCompleted(); //1 read for every profile measure
            }
            int percentComplete = completionTracker.readWriteTaskCompleted(); // do the last read outside loop to get back completion percentage
            logProgress(progressListener, progressMsg, percentComplete);
            CsvProfileObjectMapper.serializeDataToCsvFile(csvWritePath, profileSamples);
        }
        catch (IOException e)
        {
            String failMsg = "Failed to write " +  seriesIdsString + " to CSV!" + " Error: " + e.getMessage();
            if(progressListener != null)
            {
                progressListener.progress(failMsg, ProgressListener.MessageType.ERROR);
            }
            logFileLogger.log(failMsg);
            LOGGER.config(() -> failMsg);
        }
    }

    private boolean isSingleThreaded()
    {
        String useSingleThreadString = System.getProperty(MERLIN_TO_CSV_PROFILE_WRITE_SINGLE_THREAD_PROPERTY_KEY);

        boolean useSingleThreading = false;
        if(useSingleThreadString != null)
        {
            useSingleThreading = Boolean.parseBoolean(useSingleThreadString);
            if(!_loggedThreadProperty.getAndSet(true))
            {
                boolean actualValue = useSingleThreading;
                LOGGER.log(Level.CONFIG, () -> "Merlin to csv-profile write with single thread using System Property " + MERLIN_TO_CSV_PROFILE_WRITE_SINGLE_THREAD_PROPERTY_KEY
                        + " set to: " + useSingleThreadString + ". Parsed value: " + actualValue);
            }
        }
        else if(!_loggedThreadProperty.getAndSet(true))
        {
            LOGGER.log(Level.INFO, () -> "Merlin to csv-profile write with single thread using System Property " + MERLIN_TO_CSV_PROFILE_WRITE_SINGLE_THREAD_PROPERTY_KEY
                    + " is not set. Defaulting to : False");
        }
        return useSingleThreading;
    }

    private void logProgress(ProgressListener progressListener, String message, int percentComplete)
    {
        if(progressListener != null)
        {
            progressListener.progress(message, ProgressListener.MessageType.GENERAL, percentComplete);
        }
    }

}
