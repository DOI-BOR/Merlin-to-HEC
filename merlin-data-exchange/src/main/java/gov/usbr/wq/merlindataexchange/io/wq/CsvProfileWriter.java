package gov.usbr.wq.merlindataexchange.io.wq;

import gov.usbr.wq.dataaccess.model.MeasureWrapper;
import gov.usbr.wq.merlindataexchange.DataExchangeCache;
import gov.usbr.wq.merlindataexchange.MerlinDataExchangeLogBody;
import gov.usbr.wq.merlindataexchange.MerlinExchangeCompletionTracker;
import gov.usbr.wq.merlindataexchange.configuration.DataExchangeSet;
import gov.usbr.wq.merlindataexchange.configuration.DataStore;
import gov.usbr.wq.merlindataexchange.io.CloseableReentrantLock;
import gov.usbr.wq.merlindataexchange.io.DataExchangeWriter;
import gov.usbr.wq.merlindataexchange.io.ReadWriteLockManager;
import gov.usbr.wq.merlindataexchange.io.ReadWriteTimestampUtil;
import gov.usbr.wq.merlindataexchange.parameters.MerlinParameters;
import gov.usbr.wq.merlindataexchange.parameters.MerlinProfileParameters;
import hec.ui.ProgressListener;
import rma.services.annotations.ServiceProvider;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;
import java.util.logging.Logger;

import static java.util.stream.Collectors.toList;

@ServiceProvider(service = DataExchangeWriter.class, position = 200, path = DataExchangeWriter.LOOKUP_PATH
        + "/" + MerlinDataExchangeProfileReader.PROFILE + "/" + CsvProfileWriter.CSV)
public final class CsvProfileWriter implements DataExchangeWriter<MerlinProfileParameters, ProfileSampleSet>
{
    private static final Logger LOGGER = Logger.getLogger(CsvProfileWriter.class.getName());
    public static final String CSV = "csv";
    public static final String MERLIN_TO_CSV_PROFILE_WRITE_SINGLE_THREAD_PROPERTY_KEY = "merlin.dataexchange.writer.csv.profile.singlethread";
    public static final String YEAR_TAG = "<year>";
    public static final String STATION_TAG = "<station>";
    private final AtomicBoolean _loggedThreadProperty = new AtomicBoolean(false);

    @Override
    public void writeData(ProfileSampleSet profileSamples, MeasureWrapper measure, DataExchangeSet set, MerlinProfileParameters runtimeParameters,
                          DataExchangeCache cache, DataStore destinationDataStore, MerlinExchangeCompletionTracker completionTracker, ProgressListener progressListener,
                          MerlinDataExchangeLogBody logFileLogger, AtomicBoolean isCancelled, AtomicReference<String> readDurationString)
    {
        if(profileSamples == null)
        {
            return;
        }
        String csvWritePath = getDestinationPath(destinationDataStore, runtimeParameters);
        boolean useSingleThreading = isSingleThreaded();
        Instant writeStart;
        Instant writeEnd;
        List<Path> csvWritePaths;
        if(useSingleThreading)
        {
            try(CloseableReentrantLock lock = ReadWriteLockManager.getInstance().getCloseableLock().lockIt())
            {
                writeStart = Instant.now();
                csvWritePaths = writeCsv(profileSamples, csvWritePath, measure, set, cache, completionTracker, logFileLogger, progressListener, readDurationString);
                writeEnd = Instant.now();
            }
        }
        else
        {
            writeStart = Instant.now();
            csvWritePaths = writeCsv(profileSamples, csvWritePath, measure, set, cache, completionTracker, logFileLogger, progressListener, readDurationString);
            writeEnd = Instant.now();
        }
        List<String> pathStrings = csvWritePaths.stream().map(Path::toString).collect(toList());
        String successMsg = "Write to " + String.join(",\n", pathStrings) + " from " + measure.getSeriesString() + ReadWriteTimestampUtil.getDuration(writeStart, writeEnd);
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

    private List<Path> writeCsv(ProfileSampleSet profileSamples, String csvWritePath, MeasureWrapper measure, DataExchangeSet set, DataExchangeCache cache,
                                MerlinExchangeCompletionTracker completionTracker, MerlinDataExchangeLogBody logFileLogger, ProgressListener progressListener,
                                AtomicReference<String> readDurationString)
    {
        List<Path> writePaths = new ArrayList<>();
        List<String> seriesIdList = ProfileMeasuresUtil.getMeasuresListForDepthMeasure(measure, set, cache)
                .stream()
                .map(MeasureWrapper::getSeriesString)
                .collect(toList());
        String seriesIdsString = String.join(",\n", seriesIdList);
        try
        {
            int totalSize = profileSamples.stream()
                    .mapToInt(sample -> sample.getConstituents().stream()
                        .mapToInt(cdl -> cdl.getDataValues().size())
                        .sum())
                    .sum();
            String progressMsg = "Read " + seriesIdsString + " | Is processed: "
                    + measure.isProcessed() + " | Values read: " + totalSize + readDurationString;
            logFileLogger.log(progressMsg);
            for(int i = 1; i < profileSamples.first().getConstituents().size(); i++)
            {
                completionTracker.readWriteTaskCompleted(); //1 read for every profile measure
            }
            int percentComplete = completionTracker.readWriteTaskCompleted(); // do the last read outside loop to get back completion percentage
            logProgress(progressListener, progressMsg, percentComplete);
            Map<Integer, SortedSet<ProfileSample>> splitSamplesByYear = splitByYear(profileSamples);
            for(Map.Entry<Integer, SortedSet<ProfileSample>> entry : splitSamplesByYear.entrySet())
            {
                Integer year = entry.getKey();
                SortedSet<ProfileSample> samples = entry.getValue();
                String csvWritePathWithYear = csvWritePath.replace(YEAR_TAG, String.valueOf(year));
                String station = profileSamples.getStation();
                if(station != null)
                {
                    csvWritePathWithYear = csvWritePathWithYear.replace(STATION_TAG, station.replace(" ", "_"));
                }
                Path writePath = Paths.get(csvWritePathWithYear);
                CsvProfileObjectMapper.serializeDataToCsvFile(writePath, samples);
                writePaths.add(writePath);
            }
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
        return writePaths;
    }

    private String getFileNameWithoutExtension(Path csvWritePath)
    {
        String fileName = csvWritePath.toString();
        int pos = fileName.lastIndexOf(".");
        if (pos > 0 && pos < (fileName.length() - 1))
        {
            fileName = fileName.substring(0, pos);
        }
        return fileName;
    }

    private Map<Integer, SortedSet<ProfileSample>> splitByYear(SortedSet<ProfileSample> profileSamples)
    {
        // Create a new TreeMap to store the split profile samples
        Map<Integer, SortedSet<ProfileSample>> splitProfileSamples = new TreeMap<>();

        // Iterate over each ProfileSample in the input set
        for (ProfileSample profileSample : profileSamples)
        {
            ZonedDateTime dateTime = profileSample.getDateTime();
            // Get the year as a string using a DateTimeFormatter
            Integer yearString = dateTime.getYear();
            splitProfileSamples.computeIfAbsent(yearString, k -> new TreeSet<>()).add(profileSample);
        }

        return splitProfileSamples;
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

    @Override
    public String getDestinationPath(DataStore destinationDataStore, MerlinParameters parameters)
    {
        String destPath = DataExchangeWriter.super.getDestinationPath(destinationDataStore, parameters);
        String fileName = getFileNameWithoutExtension(Paths.get(destPath));
        return fileName + "-" + STATION_TAG + "-" + YEAR_TAG + ".csv";
    }
}
