package gov.usbr.wq.merlindataexchange.io;

import gov.usbr.wq.dataaccess.MerlinTimeSeriesDataAccess;
import gov.usbr.wq.dataaccess.http.ApiConnectionInfo;
import gov.usbr.wq.dataaccess.http.HttpAccessException;
import gov.usbr.wq.dataaccess.http.TokenContainer;
import gov.usbr.wq.dataaccess.model.DataWrapper;
import gov.usbr.wq.dataaccess.model.MeasureWrapper;
import gov.usbr.wq.merlindataexchange.DataExchangeCache;
import gov.usbr.wq.merlindataexchange.MerlinDataExchangeLogBody;
import gov.usbr.wq.merlindataexchange.NoEventsException;
import gov.usbr.wq.merlindataexchange.MerlinExchangeCompletionTracker;
import gov.usbr.wq.merlindataexchange.configuration.DataExchangeSet;
import gov.usbr.wq.merlindataexchange.configuration.DataStore;
import gov.usbr.wq.merlindataexchange.parameters.MerlinTimeSeriesParameters;
import hec.data.DataSetIllegalArgumentException;
import hec.heclib.dss.HecTimeSeriesBase;
import hec.heclib.util.HecTime;
import hec.hecmath.HecMathException;
import hec.io.TimeSeriesContainer;
import hec.ui.ProgressListener;
import java.util.LinkedHashSet;
import java.util.Set;
import rma.services.annotations.ServiceProvider;

import java.io.IOException;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;
import java.util.logging.Logger;

import static java.util.stream.Collectors.toList;

@ServiceProvider(service = DataExchangeReader.class, position = 100, path = DataExchangeReader.LOOKUP_PATH
        + "/" + MerlinDataExchangeReader.MERLIN + "/" + MerlinDataExchangeTimeSeriesReader.TIMESERIES)
public final class MerlinDataExchangeTimeSeriesReader extends MerlinDataExchangeReader<MerlinTimeSeriesParameters, DataWrapper, TimeSeriesContainer>
{
    private static final String AUTO_TYPE = "auto";
    private static final String STEP_TYPE = "step";
    public static final String TIMESERIES = "time-series"; //this corresponds to data-type in set we are reading for
    private static final Logger LOGGER = Logger.getLogger(MerlinDataExchangeTimeSeriesReader.class.getName());

    @Override
    protected TimeSeriesContainer convertToType(DataWrapper data, DataStore sourceDataStore, String unitSystemToConvertTo, MerlinTimeSeriesParameters parameters,
                                                ProgressListener progressListener, MerlinDataExchangeLogBody logFileLogger,
                                                MerlinExchangeCompletionTracker completionTracker, Boolean isProcessed, Instant start, Instant end, AtomicReference<String> readDurationString,
                                                MeasureWrapper measure)
    {
        TimeSeriesContainer retVal = null;
        String fPartOverride = parameters.getFPartOverride();
        try
        {
            retVal =  MerlinDataConverter.dataToTimeSeries(data, unitSystemToConvertTo, fPartOverride, isProcessed, progressListener, measure.getTypeId());
        }
        catch (MerlinInvalidTimestepException e)
        {
            String msg = "Skipping " + data.getSeriesId() + " with unsupported timestep: " + data.getTimestep() + " | Is processed: " + isProcessed;
            logFileLogger.log(msg);
            logProgressMessage(progressListener, msg);
            LOGGER.log(Level.CONFIG, e, () -> "Unsupported timestep: " + data.getTimestep());
        }
        catch (NoEventsException e)
        {
            if(start == null)
            {
                start = data.getStartTime().toInstant();
            }
            if(end == null)
            {
                end = data.getEndTime().toInstant();
            }
            Instant startDetermined = start;
            Instant endDetermined = end;
            ZoneId zoneId = data.getTimeZone();
            try
            {
                int timeStep = MerlinDataConverter.getValidTimeStep(data.getTimestep(), data.getSeriesId());
                int expectedNumValues = ExpectedNumberValuesCalculator.getExpectedNumValues(start, end,
                        HecTimeSeriesBase.getEPartFromInterval(timeStep), zoneId,
                        HecTime.fromZonedDateTime(ZonedDateTime.ofInstant(start, zoneId)), HecTime.fromZonedDateTime(ZonedDateTime.ofInstant(end, zoneId)));
                String progressMsg = "Read " + data.getSeriesId() + " | Is processed: " + isProcessed + " | Values read: 0"
                        + ", 0 missing, " + expectedNumValues + " expected" + readDurationString.get();
                String noDataMsg = e.getMessage();
                //when no data is found we count it as a read + no data written completed and move on, but want to ensure these get written together.
                int readPercentIncrement = completionTracker.readWriteTaskCompleted();
                int nothingToWritePercentIncrement = completionTracker.readWriteTaskCompleted();
                logProgressMessage(progressListener, progressMsg, readPercentIncrement);
                logProgressMessage(progressListener, noDataMsg, nothingToWritePercentIncrement);
                logFileLogger.log(progressMsg);
                logFileLogger.log(noDataMsg);
                LOGGER.log(Level.CONFIG, e, () -> "No events for " + data.getSeriesId() + " in time window " + startDetermined + " | " + endDetermined);
            }
            catch (MerlinInvalidTimestepException ex)
            {
                LOGGER.log(Level.FINE, ex.getMessage(), ex);
            }
        }
        catch (DataSetIllegalArgumentException | HecMathException e)
        {
            logError(progressListener, logFileLogger, "Failed to convert data to timeseries: " + e.getMessage(), e);
        }
        return retVal;
    }

    @Override
    protected DataWrapper retrieveData(Instant start, Instant end, DataExchangeSet dataExchangeSet, DataExchangeCache cache, String merlinApiRoot, TokenContainer token, MeasureWrapper measure,
                                       Integer qualityVersionId, DataStore sourceDataStore, ProgressListener progressListener, MerlinDataExchangeLogBody logFileLogger,
                                       AtomicBoolean isCancelled)
    {
            MerlinTimeSeriesDataAccess access = new MerlinTimeSeriesDataAccess();
            DataWrapper retVal = null;
            if(!isCancelled.get())
            {
                try
                {
                    retVal = access.getEventsBySeries(new ApiConnectionInfo(merlinApiRoot), token, measure, qualityVersionId, start, end);
                }
                catch (IOException | HttpAccessException ex)
                {
                    logError(progressListener, logFileLogger, "Failed to retrieve data for measure with series string: " + measure.getSeriesString(), ex);
                }
            }
            return retVal;
    }

    @Override
    public List<MeasureWrapper> filterMeasuresToRead(DataExchangeSet dataExchangeSet, List<MeasureWrapper> measures)
    {
        //filter out profile data for time series
        Set<String> supportedTypes = new LinkedHashSet<>(dataExchangeSet.getSupportedTypes());
        //support auto and step by default
        supportedTypes.add(AUTO_TYPE);
        supportedTypes.add(STEP_TYPE);
        return measures.stream().filter(m -> supportedTypes.contains(m.getType().toLowerCase()))
                .collect(toList());
    }
}
