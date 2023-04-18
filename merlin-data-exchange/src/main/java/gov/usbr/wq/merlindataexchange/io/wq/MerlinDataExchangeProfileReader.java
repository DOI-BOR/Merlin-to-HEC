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
import hec.data.Parameter;
import hec.data.Units;
import hec.data.UnitsConversionException;
import hec.heclib.util.Unit;
import hec.ui.ProgressListener;
import rma.services.annotations.ServiceProvider;

import java.io.IOException;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.NavigableSet;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;
import java.util.logging.Logger;

import static gov.usbr.wq.merlindataexchange.io.wq.ProfileMeasuresUtil.getMeasuresListForDepthMeasure;
import static java.util.stream.Collectors.toList;

@ServiceProvider(service = DataExchangeReader.class, position = 200, path = DataExchangeReader.LOOKUP_PATH
        + "/" + MerlinDataExchangeReader.MERLIN + "/" + MerlinDataExchangeProfileReader.PROFILE)
public final class MerlinDataExchangeProfileReader extends MerlinDataExchangeReader<MerlinProfileDataWrappers, List<ProfileSample>>
{
    public static final String PROFILE = "profile";
    private static final Logger LOGGER = Logger.getLogger(MerlinDataExchangeProfileReader.class.getName());

    @Override
    protected List<ProfileSample> convertToType(MerlinProfileDataWrappers dataWrappers, DataStore dataStore, String unitSystemToConvertTo,
                                          String fPartOverride, ProgressListener progressListener, MerlinDataExchangeLogBody logFileLogger,
                                          MerlinExchangeCompletionTracker completionTracker, Boolean isProcessed,
                                          Instant start, Instant end, AtomicReference<String> readDurationString)
    {
        List<ProfileSample> retVal = null;
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
                    ProfileConstituent profileConstituent = buildProfileConstituentData(dataValues, unitSystemToConvertTo, dataWrapper, dataStore);
                    profileConstituents.add(profileConstituent);
                }
                List<ZonedDateTime> readingDateTimes = dataWrappers.get(0).getEvents()
                        .stream()
                        .map(EventWrapper::getDate)
                        .collect(toList());
                int maxTimeStep = getMaxTimeStep(dataWrappers.get(0).getTimestep());
                retVal = ProfileDataConverter.splitDataIntoProfileSamples(profileConstituents, readingDateTimes, maxTimeStep, dataWrappers.removeFirstProfile(), dataWrappers.removeLastProfile());
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

    private int getMaxTimeStep(String timeSteps)
    {
        int maxTimeStep;
        if(timeSteps.contains(","))
        {
            String[] split = timeSteps.split(",");
            maxTimeStep = Integer.parseInt(split[0].trim());
            for(int i=1; i < split.length; i++)
            {
                int step = Integer.parseInt(split[i].trim());
                if(step > maxTimeStep)
                {
                    maxTimeStep = step;
                }
            }
        }
        else
        {
            maxTimeStep = Integer.parseInt(timeSteps.trim());
        }
        return maxTimeStep;
    }

    private int getMinTimeStep(String timeSteps)
    {
        int minTimeStep;
        if(timeSteps.contains(","))
        {
            String[] split = timeSteps.split(",");
            minTimeStep = Integer.parseInt(split[0].trim());
            for(int i=1; i < split.length; i++)
            {
                int step = Integer.parseInt(split[i].trim());
                if(step < minTimeStep)
                {
                    minTimeStep = step;
                }
            }
        }
        else
        {
            minTimeStep = Integer.parseInt(timeSteps.trim());
        }
        return minTimeStep;
    }

    private ProfileConstituent buildProfileConstituentData(List<Double> dataValues, String unitSystemToConvertTo, DataWrapper dataWrapper, DataStore dataStore) throws UnitsConversionException
    {
        Constituent constituent = ((DataStoreProfile) dataStore).getConstituentByParameter(dataWrapper.getParameter());
        int convertToUnitSystemId = getUnitSystemIdForUnitSystem(unitSystemToConvertTo);
        String unit = dataWrapper.getUnits();
        if(constituent != null)
        {
            String constituentUnit = constituent.getUnit();
            if(unit != null)
            {
                unit = constituentUnit;
                int constituentUnitSystemId = Units.getUnitSystemForUnits(constituentUnit);
                if(constituentUnitSystemId != Unit.UNDEF_ID)
                {
                    convertToUnitSystemId = constituentUnitSystemId;
                }
            }
        }
        Parameter paramId = Parameter.getParameterForUnitsString(unit); //param Id from constituent if unit defined, else from merlin
        if(convertToUnitSystemId == Unit.UNDEF_ID) //if no unit system defined in set or datastore
        {
            convertToUnitSystemId = Units.getUnitSystemForUnits(dataWrapper.getUnits()); //use one from merlin
        }
        //convert the units
        dataValues = convertUnits(dataValues, paramId.getParameterId(), dataWrapper.getUnits(), convertToUnitSystemId);
        String convertedUnit = Parameter.getUnitsStringForSystem(paramId.getParameterId(), convertToUnitSystemId);
        return new ProfileConstituent(dataWrapper.getParameter(), dataValues, convertedUnit);
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

    private List<Double> convertUnits(List<Double> dataList, int paramId, String units, int unitSystemToConvertTo) throws UnitsConversionException
    {
        List<Double> retVal = new ArrayList<>(dataList);
        int nativeUnitSystem = Units.getUnitSystemForUnits(units);
        if(nativeUnitSystem != unitSystemToConvertTo)
        {
            retVal.clear();
            for(Double data : dataList)
            {
                retVal.add(Units.convertUnits(data, paramId, nativeUnitSystem, unitSystemToConvertTo));
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
                List<MeasureWrapper> measureWrappers = getMeasuresListForDepthMeasure(depthMeasure, dataExchangeSet, cache);
                for(MeasureWrapper measureWrapper: measureWrappers)
                {
                    DataWrapper data = access.getEventsBySeries(new ApiConnectionInfo(merlinApiRoot), token, measureWrapper, qualityVersionId, start, end);
                    retVal.add(data);
                }
                determineRemovalOfFirstAndLastProfiles(access, merlinApiRoot, token, depthMeasure, qualityVersionId, start, end, retVal);
            }
            catch (IOException | HttpAccessException ex)
            {
                logError(progressListener, logFileLogger, "Failed to retrieve data for measure with series string: " + depthMeasure.getSeriesString(), ex);
            }
        }
        return retVal;
    }

    private void determineRemovalOfFirstAndLastProfiles(MerlinTimeSeriesDataAccess access, String merlinApiRoot, TokenContainer token,
                                                        MeasureWrapper depthMeasure, Integer qualityVersionId, Instant start, Instant end,
                                                        MerlinProfileDataWrappers retVal) throws IOException, HttpAccessException
    {
        if(!retVal.isEmpty())
        {
            Optional<DataWrapper> dataWrapper = retVal.stream().filter(dw -> dw.getParameter().equalsIgnoreCase(DataStoreProfile.DEPTH))
                    .findFirst();
            if(dataWrapper.isPresent() && !dataWrapper.get().getEvents().isEmpty())
            {
                DataWrapper depthData = dataWrapper.get();
                long multiple = ProfileDataConverter.getSignificantChangeTimeStepMultiple();
                long minTimeStep = getMinTimeStep(depthData.getTimestep());
                long maxTimeStep = getMaxTimeStep(depthData.getTimestep());
                long maxTimeJumpBeforeConsideredSignificantChange = maxTimeStep * multiple - minTimeStep;
                Instant newStart = start.minusSeconds(maxTimeJumpBeforeConsideredSignificantChange * 60);
                Instant newEnd = end.plusSeconds(maxTimeJumpBeforeConsideredSignificantChange *60);
                //build two new small data sets from original moving back/forward to just before what we consider to be a significant change
                DataWrapper newStartData = access.getEventsBySeries(new ApiConnectionInfo(merlinApiRoot), token, depthMeasure, qualityVersionId, newStart, start);
                DataWrapper newEndData = access.getEventsBySeries(new ApiConnectionInfo(merlinApiRoot), token, depthMeasure, qualityVersionId, end, newEnd);
                List<EventWrapper> newStartEvents = new ArrayList<>(newStartData.getEvents());
                List<EventWrapper> newEndEvents =  new ArrayList<>(newEndData.getEvents());
                EventWrapper oldFirst = depthData.getEvents().first();
                EventWrapper oldLast = depthData.getEvents().last();
                //if there is data in our new small data sets, it means that there was NON-significant change data time-wise that got cut off by the time window
                //so lets check the depths of those data to see if there was a significant change there
                //if not, we should remove this data set as it doesn't contain complete data for the profile
                if(newStartEvents.size() > 1)
                {
                    EventWrapper beforeOriginalFirst = newStartEvents.get(newStartEvents.size() - 2);
                    if(!ProfileDataConverter.isDifferenceSignificantChange(beforeOriginalFirst.getDate(), oldFirst.getDate(), maxTimeStep, beforeOriginalFirst.getValue(), oldFirst.getValue()))
                    {
                        retVal.setRemoveFirstProfile();
                    }
                }
                if(newEndEvents.size() > 1)
                {
                    EventWrapper afterOriginalLast = newEndEvents.get(1);
                    if(!ProfileDataConverter.isDifferenceSignificantChange(oldLast.getDate(), afterOriginalLast.getDate(), maxTimeStep, oldLast.getValue(), afterOriginalLast.getValue()))
                    {
                        retVal.setRemoveLastProfile();
                    }
                }
            }
        }
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
