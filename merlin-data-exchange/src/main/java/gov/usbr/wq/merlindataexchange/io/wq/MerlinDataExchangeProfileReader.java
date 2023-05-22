package gov.usbr.wq.merlindataexchange.io.wq;

import gov.usbr.wq.dataaccess.MerlinTimeSeriesDataAccess;
import gov.usbr.wq.dataaccess.http.ApiConnectionInfo;
import gov.usbr.wq.dataaccess.http.HttpAccessException;
import gov.usbr.wq.dataaccess.http.TokenContainer;
import gov.usbr.wq.dataaccess.model.DataWrapper;
import gov.usbr.wq.dataaccess.model.EventWrapper;
import gov.usbr.wq.dataaccess.model.MeasureWrapper;
import gov.usbr.wq.merlindataexchange.DataExchangeCache;
import gov.usbr.wq.merlindataexchange.MerlinDataExchangeLogBody;
import gov.usbr.wq.merlindataexchange.MerlinExchangeCompletionTracker;
import gov.usbr.wq.merlindataexchange.NoEventsException;
import gov.usbr.wq.merlindataexchange.configuration.Constituent;
import gov.usbr.wq.merlindataexchange.configuration.DataExchangeSet;
import gov.usbr.wq.merlindataexchange.configuration.DataStore;
import gov.usbr.wq.merlindataexchange.configuration.DataStoreProfile;
import gov.usbr.wq.merlindataexchange.io.DataExchangeReader;
import gov.usbr.wq.merlindataexchange.io.MerlinDataExchangeReader;
import gov.usbr.wq.merlindataexchange.parameters.MerlinProfileParameters;
import hec.data.Parameter;
import hec.data.Units;
import hec.data.UnitsConversionException;
import hec.heclib.util.Unit;
import hec.ui.ProgressListener;
import rma.services.annotations.ServiceProvider;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.NavigableSet;
import java.util.Optional;
import java.util.SortedSet;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;
import java.util.logging.Logger;

import static gov.usbr.wq.merlindataexchange.io.wq.ProfileMeasuresUtil.getMeasuresListForDepthMeasure;
import static java.util.stream.Collectors.toList;

@ServiceProvider(service = DataExchangeReader.class, position = 200, path = DataExchangeReader.LOOKUP_PATH
        + "/" + MerlinDataExchangeReader.MERLIN + "/" + MerlinDataExchangeProfileReader.PROFILE)
public final class MerlinDataExchangeProfileReader extends MerlinDataExchangeReader<MerlinProfileParameters, MerlinProfileDataWrappers, SortedSet<ProfileSample>>
{
    public static final String PROFILE = "profile";
    private static final Logger LOGGER = Logger.getLogger(MerlinDataExchangeProfileReader.class.getName());

    @Override
    protected SortedSet<ProfileSample> convertToType(MerlinProfileDataWrappers dataWrappers, DataStore dataStore, String unitSystemToConvertTo,
                                                     MerlinProfileParameters parameters, ProgressListener progressListener, MerlinDataExchangeLogBody logFileLogger,
                                                     MerlinExchangeCompletionTracker completionTracker, Boolean isProcessed,
                                                     Instant start, Instant end, AtomicReference<String> readDurationString)
    {
        SortedSet<ProfileSample> retVal = null;
        if(!dataWrappers.isEmpty())
        {
            try
            {
                List<ProfileConstituent> profileConstituents = new ArrayList<>();
                for(DataWrapper dataWrapper : dataWrappers)
                {
                    NavigableSet<EventWrapper> events = dataWrapper.getEvents();
                    if(dataWrapper.getParameter().equalsIgnoreCase(DataStoreProfile.DEPTH) && events.isEmpty())
                    {
                        throw new NoEventsException("CSV", dataWrapper.getSeriesId());
                    }
                    List<Double> dataValues = dataWrapper.getEvents().stream().map(EventWrapper::getValue)
                            .collect(toList());
                    List<ZonedDateTime> dateValues = dataWrapper.getEvents().stream().map(EventWrapper::getDate)
                            .collect(toList());
                    ProfileConstituent profileConstituent = buildProfileConstituentData(dataValues, dateValues, unitSystemToConvertTo, dataWrapper, dataStore);
                    profileConstituents.add(profileConstituent);
                }
                List<ZonedDateTime> readingDateTimes = dataWrappers.get(0).getEvents()
                        .stream()
                        .map(EventWrapper::getDate)
                        .collect(toList());
                retVal = ProfileDataConverter.splitDataIntoProfileSamples(profileConstituents, readingDateTimes, dataWrappers.removeFirstProfile(), dataWrappers.removeLastProfile());
            }
            catch (NoEventsException e)
            {
                handleNoEventsForData(dataWrappers, start, end, isProcessed, readDurationString, completionTracker, logFileLogger, progressListener, e);
            }
            catch (UnitsConversionException e)
            {
                logUnitConversionError(e, progressListener);
            }
        }
        return retVal;
    }

    private ProfileConstituent buildProfileConstituentData(List<Double> dataValues, List<ZonedDateTime> dateValues, String unitSystemToConvertTo, DataWrapper dataWrapper, DataStore dataStore) throws UnitsConversionException
    {
        Constituent constituent = ((DataStoreProfile) dataStore).getConstituentByParameter(dataWrapper.getParameter());
        int convertToUnitSystemId = getUnitSystemIdForUnitSystem(unitSystemToConvertTo);
        String unitToConvertTo = dataWrapper.getUnits();
        String constituentUnit = null;
        String unitFromTemplateUnitSystem = null;
        if(constituent != null)
        {
            constituentUnit = constituent.getUnit();
        }
        unitFromTemplateUnitSystem = Parameter.getUnitsStringForSystem(dataWrapper.getParameter(), convertToUnitSystemId);
        if(constituentUnit != null)
        {
            unitToConvertTo = constituentUnit;
        }
        else if(!Units.UNDEFINED_UNITS.equalsIgnoreCase(unitFromTemplateUnitSystem))
        {
            unitToConvertTo = unitFromTemplateUnitSystem;
        }
        dataValues = convertUnits(dataValues, dataWrapper.getUnits(), unitToConvertTo);
        return new ProfileConstituent(dataWrapper.getParameter(), dataValues, dateValues, unitToConvertTo);
    }

    private int getUnitSystemIdForUnitSystem(String unitSystemToConvertTo)
    {
        int convertToUnitSystemId = Unit.UNDEF_ID;
        if (Unit.ENGLISH.equalsIgnoreCase(unitSystemToConvertTo))
        {
            convertToUnitSystemId = Unit.ENGLISH_ID;
        }
        else if (Unit.SI.equalsIgnoreCase(unitSystemToConvertTo))
        {
            convertToUnitSystemId = Unit.SI_ID;
        }
        return convertToUnitSystemId;
    }

    private List<Double> convertUnits(List<Double> dataList, String unitsFrom, String unitsTo) throws UnitsConversionException
    {
        List<Double> retVal = new ArrayList<>(dataList);
        if(!unitsFrom.equalsIgnoreCase(unitsTo))
        {
            retVal.clear();
            for(Double data : dataList)
            {
                retVal.add(Units.convertUnits(data, unitsFrom, unitsTo));
            }
        }
        return retVal;
    }

    private void logUnitConversionError(Exception e, ProgressListener progressListener)
    {
        LOGGER.log(Level.CONFIG, e, () -> "Failed to determine units to convert to");
        if (progressListener != null)
        {
            progressListener.progress("Failed to determine units to convert to", ProgressListener.MessageType.ERROR);
        }
    }

    private void handleNoEventsForData(List<DataWrapper> dataWrappers, Instant start, Instant end, Boolean isProcessed, AtomicReference<String> readDurationString,
                                       MerlinExchangeCompletionTracker completionTracker, MerlinDataExchangeLogBody logFileLogger, ProgressListener progressListener, NoEventsException e)
    {
        List<String> seriesIds = dataWrappers.stream().map(DataWrapper::getSeriesId).collect(toList());
        String seriesIdsString = String.join(",\n", seriesIds);
        String progressMsg = "Read " + seriesIdsString + " | Is processed: " + isProcessed + " | Values read: 0"
                 + readDurationString.get();
        String noDataMsg = e.getMessage();
        DataWrapper firstData = dataWrappers.get(0);
        if(start == null)
        {
            start = firstData.getStartTime().toInstant();
        }
        if(end == null)
        {
            end = firstData.getEndTime().toInstant();
        }
        Instant startDetermined = start;
        Instant endDetermined = end;
        //when no data is found we count it as a read + no data written completed and move on, but want to ensure these get written together.
        for(int i=1; i < dataWrappers.size(); i++)
        {
            completionTracker.readWriteTaskCompleted();
            completionTracker.readWriteTaskCompleted();
        }
        int readPercentIncrement = completionTracker.readWriteTaskCompleted();
        int nothingToWritePercentIncrement = completionTracker.readWriteTaskCompleted();
        logProgressMessage(progressListener, progressMsg, readPercentIncrement);
        logProgressMessage(progressListener, noDataMsg, nothingToWritePercentIncrement);
        logFileLogger.log(progressMsg);
        logFileLogger.log(noDataMsg);
        LOGGER.log(Level.CONFIG, e, () -> "No events for " + seriesIdsString + " in time window " + startDetermined + " | " + endDetermined);
    }

    @Override
    protected MerlinProfileDataWrappers retrieveData(Instant start, Instant end, DataExchangeSet dataExchangeSet, DataExchangeCache cache, String merlinApiRoot, TokenContainer token,
                                                      MeasureWrapper depthMeasure, Integer qualityVersionId, DataStore sourceDataStore, ProgressListener progressListener,
                                                      MerlinDataExchangeLogBody logFileLogger, AtomicBoolean isCancelled)
    {

        MerlinTimeSeriesDataAccess access = new MerlinTimeSeriesDataAccess();
        MerlinProfileDataWrappers retVal =  new MerlinProfileDataWrappers();
        if(!isCancelled.get())
        {
            try
            {
                start = getStartOfYearInstant(start, depthMeasure.getZoneId());
                end = getEndOfYearInstant(end, depthMeasure.getZoneId());
                List<MeasureWrapper> measureWrappers = getMeasuresListForDepthMeasure(depthMeasure, dataExchangeSet, cache);
                Duration significantTimeChange = ProfileDataConverter.getSignificantTimeChange();
                long maxTimeJumpBeforeConsideredSignificantChange = significantTimeChange.toMinutes() - 1;
                Instant expandedStart = start.minusSeconds(maxTimeJumpBeforeConsideredSignificantChange * 60);
                Instant expandedEnd = end.plusSeconds(maxTimeJumpBeforeConsideredSignificantChange *60);
                for(MeasureWrapper measureWrapper: measureWrappers)
                {
                    DataWrapper data = access.getEventsBySeries(new ApiConnectionInfo(merlinApiRoot), token, measureWrapper, qualityVersionId, expandedStart, expandedEnd);
                    retVal.add(data);
                }
                determineRemovalOfFirstAndLastProfiles(start, end, retVal);
            }
            catch (IOException | HttpAccessException ex)
            {
                logError(progressListener, logFileLogger, "Failed to retrieve data for measure with series string: " + depthMeasure.getSeriesString(), ex);
            }
        }
        return retVal;
    }

    private void determineRemovalOfFirstAndLastProfiles(Instant start, Instant end, MerlinProfileDataWrappers retVal)
    {
        if(!retVal.isEmpty())
        {
            Optional<DataWrapper> dataWrapper = retVal.stream().filter(dw -> dw.getParameter().equalsIgnoreCase(DataStoreProfile.DEPTH))
                    .findFirst();
            if(dataWrapper.isPresent() && !dataWrapper.get().getEvents().isEmpty())
            {
                DataWrapper depthData = dataWrapper.get();
                List<EventWrapper> events = new ArrayList<>(depthData.getEvents());
                Optional<EventWrapper> originalStartEvent = events.stream().filter(e -> e.getDate().toInstant().equals(start) || e.getDate().toInstant().isAfter(start))
                        .findFirst();
                Optional<EventWrapper> originalEndEvent = getOriginalEndEvent(events, end);
                if(events.size() > 2)
                {
                    List<Double> depths = depthData.getEvents().stream().map(EventWrapper::getValue).collect(toList());
                    Double min = ProfileMeasuresUtil.getMinDepth(depths);
                    Double max = ProfileMeasuresUtil.getMaxDepth(depths);
                    originalStartEvent.ifPresent(eventWrapper -> determineRemovalOfFirst(eventWrapper, start, events, min, max, retVal));
                    originalEndEvent.ifPresent(eventWrapper -> determineRemovalOfLast(eventWrapper, end, events, min, max, retVal));
                }
            }
        }
    }

    private Instant getStartOfYearInstant(Instant instant, ZoneId zoneId)
    {
        ZonedDateTime zonedDateTime = instant.atZone(zoneId);
        int year = zonedDateTime.getYear();
        ZonedDateTime startOfYear = ZonedDateTime.of(year, 1, 1, 0, 0, 0, 0, zoneId);
        startOfYear = startOfYear.minus(ProfileDataConverter.getSignificantTimeChange());
        return startOfYear.toInstant();
    }
    private Instant getEndOfYearInstant(Instant instant, ZoneId zoneId)
    {
        ZonedDateTime zonedDateTime = instant.atZone(zoneId);
        int year = zonedDateTime.getYear();
        ZonedDateTime startOfYear = ZonedDateTime.of(year+1, 1, 1, 0, 0, 0, 0, zoneId);
        startOfYear = startOfYear.plus(ProfileDataConverter.getSignificantTimeChange());
        return startOfYear.toInstant();
    }

    private void determineRemovalOfLast(EventWrapper originalEndEvent, Instant end, List<EventWrapper> events, Double min, Double max, MerlinProfileDataWrappers retVal)
    {
        int originalEndIndex = events.indexOf(originalEndEvent);
        int lastIndex = events.size() - 1;
        if(originalEndIndex < lastIndex && events.get(originalEndIndex + 1).getDate().toInstant().isAfter(end)
                && !ProfileDataConverter.isDifferenceSignificantChange(events.get(originalEndIndex).getDate(), events.get(originalEndIndex+1).getDate(),
                events.get(originalEndIndex).getValue(), events.get(originalEndIndex+1).getValue(), max, min))
        {
            retVal.setRemoveLastProfile();
        }
    }

    private void determineRemovalOfFirst(EventWrapper originalStartEvent, Instant start, List<EventWrapper> events, Double min, Double max, MerlinProfileDataWrappers retVal)
    {
        int originalStartIndex = events.indexOf(originalStartEvent);
        if(originalStartIndex > 0 && events.get(originalStartIndex-1).getDate().toInstant().isBefore(start)
                && !ProfileDataConverter.isDifferenceSignificantChange(events.get(originalStartIndex-1).getDate(), events.get(originalStartIndex).getDate(),
                events.get(originalStartIndex-1).getValue(), events.get(originalStartIndex).getValue(),
                max, min))
        {
            retVal.setRemoveFirstProfile();
        }
    }

    private Optional<EventWrapper> getOriginalEndEvent(List<EventWrapper> events, Instant end)
    {
        List<EventWrapper> eventsReversed = new ArrayList<>(events);
        Collections.reverse(eventsReversed);
        return eventsReversed.stream().filter(e -> e.getDate().toInstant().equals(end) || e.getDate().toInstant().isBefore(end))
                .findFirst();
    }

    @Override
    public List<MeasureWrapper> filterMeasuresToRead(DataExchangeSet dataExchangeSet, List<MeasureWrapper> measures)
    {
        //the reader for depth-temp profiles will handle reading 2 measures at once (one for temp, one for depth)
        //so we want to filter out all but the depth measures from the list that gets handed into the exchange as reading
        //the other constituent measures will happen internally inside the reader
       return measures.stream().filter(m -> m.getParameter().equalsIgnoreCase(DataStoreProfile.DEPTH)
                                && m.getType().equalsIgnoreCase(dataExchangeSet.getDataType()))
                        .collect(toList());
    }
}
