package gov.usbr.wq.merlindataexchange.io.wq;

import gov.usbr.wq.merlindataexchange.configuration.DataStoreProfile;

import java.time.Duration;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.SortedSet;
import java.util.TreeSet;

final class ProfileDataConverter
{

    private static final String SIGNIFICANT_TIME_CHANGE_PROPERTY_MINUTES = "merlin.dataexchange.reader.profile.significanttimechange.minutes";
    private static final String SIGNIFICANT_CHANGE_DEPTH_PERCENT_PROPERTY = "merlin.dataexchange.reader.profile.significantchange.depth.percent";

    private ProfileDataConverter()
    {
        throw new AssertionError("Utility Class. Don't instantiate");
    }

    private static ZonedDateTime calculateProfileDateTime(List<ZonedDateTime> constituentDataList)
    {
        return constituentDataList.get(0);
    }

    static ProfileSampleSet splitDataIntoProfileSamples(List<ProfileConstituent> constituents, List<ZonedDateTime> readingDateTimes,
                                                                String station, boolean removeFirstProfile, boolean removeLastProfile)
    {
        List<List<ZonedDateTime>> dateTimeGroups = separateDateTimeGroups(constituents, readingDateTimes);
        ProfileSampleSet retVal = new ProfileSampleSet(station);
        List<List<ProfileConstituent>> separatedProfileConstituents = new ArrayList<>();
        for(ProfileConstituent constituent : constituents)
        {
            List<List<Double>> separatedValuesList = separateValues(constituent.getDataValues(), dateTimeGroups);
            List<ZonedDateTime> dates = readingDateTimes;
            if(!constituent.getDateValues().isEmpty())
            {
                dates = constituent.getDateValues();
            }
            List<List<ZonedDateTime>> separatedZonedDateTimesList = separateDateTimes(dates, dateTimeGroups);
            List<ProfileConstituent> profileConstituentSubGroups = new ArrayList<>();
            int i=0;
            for(List<Double> valuesGroup : separatedValuesList)
            {
                ProfileConstituent profileConstituentSubGroup = new ProfileConstituent(constituent.getParameter(), valuesGroup, separatedZonedDateTimesList.get(i), constituent.getUnit());
                profileConstituentSubGroups.add(profileConstituentSubGroup);
                i++;
            }
            separatedProfileConstituents.add(profileConstituentSubGroups);
        }
        //go down groups
        for(int groupIndex =0; groupIndex < dateTimeGroups.size(); groupIndex++)
        {
            List<ProfileConstituent> constituentsAtGroupIndex = new ArrayList<>();
            //go across columns
            for(List<ProfileConstituent> constituentSplitUp : separatedProfileConstituents)
            {
                //collect subgroup in each column into list
                constituentsAtGroupIndex.add(constituentSplitUp.get(groupIndex));
            }
            ZonedDateTime profileSampleDateTime = calculateProfileDateTime(dateTimeGroups.get(groupIndex));
            retVal.add(new ProfileSample(profileSampleDateTime, constituentsAtGroupIndex));
        }
        if(!retVal.isEmpty() && removeFirstProfile)
        {
            retVal.remove(retVal.first());
        }
        if(!retVal.isEmpty() && removeLastProfile)
        {
            retVal.remove(retVal.last());
        }
        return retVal;
    }

    private static List<List<Double>> separateValues(List<Double> values, List<List<ZonedDateTime>> dateTimeGroups)
    {
        List<List<Double>> result = new ArrayList<>();
        int index = 0;
        for (List<ZonedDateTime> dateTimeGroup : dateTimeGroups)
        {
            int groupSize = dateTimeGroup.size();
            List<Double> sublist = new ArrayList<>();
            for (int i = 0; i < groupSize; i++)
            {
                sublist.add(values.get(index));
                index++;
            }
            result.add(sublist);
        }
        return result;
    }

    private static List<List<ZonedDateTime>> separateDateTimes(List<ZonedDateTime> dates, List<List<ZonedDateTime>> dateTimeGroups)
    {
        List<List<ZonedDateTime>> result = new ArrayList<>();
        int index = 0;
        for (List<ZonedDateTime> dateTimeGroup : dateTimeGroups)
        {
            int groupSize = dateTimeGroup.size();
            List<ZonedDateTime> sublist = new ArrayList<>();
            for (int i = 0; i < groupSize; i++)
            {
                sublist.add(dates.get(index));
                index++;
            }
            result.add(sublist);
        }
        return result;
    }

    private static List<List<ZonedDateTime>> separateDateTimeGroups(List<ProfileConstituent> constituents, List<ZonedDateTime> readingDateTimes)
    {
        List<List<ZonedDateTime>> separatedDateTimes = new ArrayList<>();
        ZonedDateTime previousTime = null;
        Optional<ProfileConstituent> depthConstituentOpt = constituents.stream()
                .filter(c -> c.getParameter().equalsIgnoreCase(DataStoreProfile.DEPTH))
                .findFirst();
        if(depthConstituentOpt.isPresent())
        {
            ProfileConstituent depthConstituent = depthConstituentOpt.get();
            Double min = ProfileMeasuresUtil.getMinDepth(depthConstituent.getDataValues());
            Double max = ProfileMeasuresUtil.getMaxDepth(depthConstituent.getDataValues());
            for (int i = 0; i < readingDateTimes.size(); i++)
            {
                ZonedDateTime currentTime = readingDateTimes.get(i);
                if (previousTime == null)
                {
                    separatedDateTimes.add(new ArrayList<>());
                    previousTime = currentTime;
                    separatedDateTimes.get(separatedDateTimes.size()-1).add(previousTime);
                }
                else if (i < depthConstituent.getDataValues().size())
                {
                    Double currentDepth = depthConstituent.getDataValues().get(i);
                    Double previousDepth = depthConstituent.getDataValues().get(i - 1);
                    if (isDifferenceSignificantChange(previousTime, currentTime, previousDepth, currentDepth, max, min))
                    {
                        separatedDateTimes.add(new ArrayList<>());
                        separatedDateTimes.get(separatedDateTimes.size()-1).add(currentTime);
                    }
                    else
                    {
                        separatedDateTimes.get(separatedDateTimes.size()-1).add(currentTime);
                    }
                    previousTime = currentTime;
                }
            }
        }
        return separatedDateTimes;
    }

    private static boolean isSignificantDepthChange(Double currentDepth, Double previousDepth, Double currentMax, Double currentMin)
    {
        boolean retVal = false;
        if(!Objects.equals(currentMax, currentDepth))
        {
            double currentDifference = Math.abs(currentDepth - previousDepth);
            double minMaxDifference = Math.abs(currentMax - currentMin);
            retVal = currentDifference > minMaxDifference * (getSignificantChangeDepthPercent()/100.0);
        }
        return retVal;
    }

    private static boolean isSignificantTimeChange(ZonedDateTime previousTime, ZonedDateTime currentTime)
    {
        Duration timeDiff = Duration.between(previousTime, currentTime);
        Duration significationTimeChange = getSignificantTimeChange();
        return timeDiff.compareTo(significationTimeChange) >= 0;
    }

    static Duration getSignificantTimeChange()
    {
        String timeChangeMinutesProperty = System.getProperty(SIGNIFICANT_TIME_CHANGE_PROPERTY_MINUTES);
        Duration timeChange = Duration.ofHours(6);
        if(timeChangeMinutesProperty != null)
        {
            int timeChangeMinutes = Integer.parseInt(timeChangeMinutesProperty);
            timeChange = Duration.ofMinutes(timeChangeMinutes);
        }
        return timeChange;
    }

    private static double getSignificantChangeDepthPercent()
    {
        String depthPercentDecreaseProperty = System.getProperty(SIGNIFICANT_CHANGE_DEPTH_PERCENT_PROPERTY);
        double depthPercentDecrease = 50.0;
        if(depthPercentDecreaseProperty != null)
        {
            depthPercentDecrease = Double.parseDouble(depthPercentDecreaseProperty);
        }
        return depthPercentDecrease;
    }

    static boolean isDifferenceSignificantChange(ZonedDateTime previousDateTime, ZonedDateTime currentDateTime, Double previousDepth, Double currentDepth,
                                                 Double currentMaxDepth, Double currentMinDepth)
    {
        return isSignificantTimeChange(previousDateTime, currentDateTime)
                || isSignificantDepthChange(currentDepth, previousDepth, currentMaxDepth, currentMinDepth);
    }

}
