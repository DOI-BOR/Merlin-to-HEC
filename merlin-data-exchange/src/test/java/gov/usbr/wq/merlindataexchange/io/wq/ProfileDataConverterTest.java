package gov.usbr.wq.merlindataexchange.io.wq;

import org.junit.jupiter.api.Test;

import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.SortedSet;

import static org.junit.jupiter.api.Assertions.*;

final class ProfileDataConverterTest
{
    @Test
    void testProfileSampleSplitCelsius()
    {
        List<ProfileConstituent> profileConstituentList = new ArrayList<>();
        ProfileConstituent depthConstituent = new ProfileConstituent("Depth", Arrays.asList(0.1,5.0,10.0,15.0,0.1,5.1,10.1,15.1), new ArrayList<>(), "ft");
        ProfileConstituent tempConstituent = new ProfileConstituent("Temp-Water", Arrays.asList(10.0,11.0,12.0,13.0,10.0,9.0,8.0,7.0), new ArrayList<>(), "C");
        profileConstituentList.add(depthConstituent);
        profileConstituentList.add(tempConstituent);
        List<ZonedDateTime> readDateTimes = Arrays.asList(ZonedDateTime.parse("2009-09-15T10:06:00-08:00", DateTimeFormatter.ISO_OFFSET_DATE_TIME),
                ZonedDateTime.parse("2009-09-15T10:06:01-08:00", DateTimeFormatter.ISO_OFFSET_DATE_TIME),
                ZonedDateTime.parse("2009-09-15T10:06:02-08:00", DateTimeFormatter.ISO_OFFSET_DATE_TIME),
                ZonedDateTime.parse("2009-09-15T10:06:04-08:00", DateTimeFormatter.ISO_OFFSET_DATE_TIME),
                ZonedDateTime.parse("2009-09-23T10:06:00-08:00", DateTimeFormatter.ISO_OFFSET_DATE_TIME),
                ZonedDateTime.parse("2009-09-23T10:06:01-08:00", DateTimeFormatter.ISO_OFFSET_DATE_TIME),
                ZonedDateTime.parse("2009-09-23T10:06:02-08:00", DateTimeFormatter.ISO_OFFSET_DATE_TIME),
                ZonedDateTime.parse("2009-09-23T10:06:04-08:00", DateTimeFormatter.ISO_OFFSET_DATE_TIME));
        SortedSet<ProfileSample> result = ProfileDataConverter.splitDataIntoProfileSamples(profileConstituentList, readDateTimes, null, false, false);
        assertEquals(2, result.size());
        assertEquals(result.first().getDateTime(), ZonedDateTime.parse("2009-09-15T10:06:00-08:00", DateTimeFormatter.ISO_OFFSET_DATE_TIME));
        List<ProfileConstituent> firstDataList = result.first().getConstituents();
        ProfileConstituent depthConstituentResult = firstDataList.get(0);
        assertEquals("Depth", depthConstituentResult.getParameter());
        assertEquals(Arrays.asList(0.1, 5.0, 10.0, 15.0), depthConstituentResult.getDataValues());
        assertEquals("ft", depthConstituentResult.getUnit());
        ProfileConstituent tempConstituentResult = firstDataList.get(1);
        assertEquals("Temp-Water", tempConstituentResult.getParameter());
        assertEquals(Arrays.asList(10.0,11.0,12.0,13.0), tempConstituentResult.getDataValues());
        assertEquals("C", tempConstituentResult.getUnit());

        List<ProfileConstituent> secondDataList = result.last().getConstituents();
        ProfileConstituent depthConstituentResult2 = secondDataList.get(0);
        assertEquals("Depth", depthConstituentResult2.getParameter());
        assertEquals(Arrays.asList(0.1, 5.1, 10.1, 15.1), depthConstituentResult2.getDataValues());
        assertEquals("ft", depthConstituentResult2.getUnit());
        ProfileConstituent tempConstituentResult2 = secondDataList.get(1);
        assertEquals("Temp-Water", tempConstituentResult2.getParameter());
        assertEquals(Arrays.asList(10.0,9.0,8.0,7.0), tempConstituentResult2.getDataValues());
        assertEquals("C", tempConstituentResult2.getUnit());
    }

    @Test
    void testProfileSampleSplitFahrenheit()
    {
        List<ProfileConstituent> profileConstituentList = new ArrayList<>();
        ProfileConstituent depthConstituent = new ProfileConstituent("Depth", Arrays.asList(0.1,5.0,10.0,15.0,0.1,5.1,10.1,15.1), new ArrayList<>(), "ft");
        ProfileConstituent tempConstituent = new ProfileConstituent("Temp-Water", Arrays.asList(10.0,11.0,12.0,13.0,10.0,9.0,8.0,7.0), new ArrayList<>(), "F");
        profileConstituentList.add(depthConstituent);
        profileConstituentList.add(tempConstituent);
        List<ZonedDateTime> readDateTimes = Arrays.asList(ZonedDateTime.parse("2009-09-15T10:06:00-08:00", DateTimeFormatter.ISO_OFFSET_DATE_TIME),
                ZonedDateTime.parse("2009-09-15T10:06:01-08:00", DateTimeFormatter.ISO_OFFSET_DATE_TIME),
                ZonedDateTime.parse("2009-09-15T10:06:02-08:00", DateTimeFormatter.ISO_OFFSET_DATE_TIME),
                ZonedDateTime.parse("2009-09-15T10:06:04-08:00", DateTimeFormatter.ISO_OFFSET_DATE_TIME),
                ZonedDateTime.parse("2009-09-23T10:06:00-08:00", DateTimeFormatter.ISO_OFFSET_DATE_TIME),
                ZonedDateTime.parse("2009-09-23T10:06:01-08:00", DateTimeFormatter.ISO_OFFSET_DATE_TIME),
                ZonedDateTime.parse("2009-09-23T10:06:02-08:00", DateTimeFormatter.ISO_OFFSET_DATE_TIME),
                ZonedDateTime.parse("2009-09-23T10:06:04-08:00", DateTimeFormatter.ISO_OFFSET_DATE_TIME));
        SortedSet<ProfileSample> result = ProfileDataConverter.splitDataIntoProfileSamples(profileConstituentList, readDateTimes, null, false, false);
        assertEquals(2, result.size());
        assertEquals(result.first().getDateTime(), ZonedDateTime.parse("2009-09-15T10:06:00-08:00", DateTimeFormatter.ISO_OFFSET_DATE_TIME));
        List<ProfileConstituent> firstDataList = result.first().getConstituents();
        ProfileConstituent depthConstituentResult = firstDataList.get(0);
        assertEquals("Depth", depthConstituentResult.getParameter());
        assertEquals(Arrays.asList(0.1, 5.0, 10.0, 15.0), depthConstituentResult.getDataValues());
        assertEquals("ft", depthConstituentResult.getUnit());
        ProfileConstituent tempConstituentResult = firstDataList.get(1);
        assertEquals("Temp-Water", tempConstituentResult.getParameter());
        assertEquals(Arrays.asList(10.0,11.0,12.0,13.0), tempConstituentResult.getDataValues());
        assertEquals("F", tempConstituentResult.getUnit());

        List<ProfileConstituent> secondDataList = result.last().getConstituents();
        ProfileConstituent depthConstituentResult2 = secondDataList.get(0);
        assertEquals("Depth", depthConstituentResult2.getParameter());
        assertEquals(Arrays.asList(0.1, 5.1, 10.1, 15.1), depthConstituentResult2.getDataValues());
        assertEquals("ft", depthConstituentResult2.getUnit());
        ProfileConstituent tempConstituentResult2 = secondDataList.get(1);
        assertEquals("Temp-Water", tempConstituentResult2.getParameter());
        assertEquals(Arrays.asList(10.0,9.0,8.0,7.0), tempConstituentResult2.getDataValues());
        assertEquals("F", tempConstituentResult2.getUnit());
    }

    @Test
    void testProfileSampleSplitKelvin()
    {
        List<ProfileConstituent> profileConstituentList = new ArrayList<>();
        ProfileConstituent depthConstituent = new ProfileConstituent("Depth", Arrays.asList(0.1,5.0,10.0,15.0,0.1,5.1,10.1,15.1), new ArrayList<>(), "ft");
        ProfileConstituent tempConstituent = new ProfileConstituent("Temp-Water", Arrays.asList(10.0,11.0,12.0,13.0,10.0,9.0,8.0,7.0), new ArrayList<>(), "K");
        profileConstituentList.add(depthConstituent);
        profileConstituentList.add(tempConstituent);
        List<ZonedDateTime> readDateTimes = Arrays.asList(ZonedDateTime.parse("2009-09-15T10:06:00-08:00", DateTimeFormatter.ISO_OFFSET_DATE_TIME),
                ZonedDateTime.parse("2009-09-15T10:06:01-08:00", DateTimeFormatter.ISO_OFFSET_DATE_TIME),
                ZonedDateTime.parse("2009-09-15T10:06:02-08:00", DateTimeFormatter.ISO_OFFSET_DATE_TIME),
                ZonedDateTime.parse("2009-09-15T10:06:04-08:00", DateTimeFormatter.ISO_OFFSET_DATE_TIME),
                ZonedDateTime.parse("2009-09-23T10:06:00-08:00", DateTimeFormatter.ISO_OFFSET_DATE_TIME),
                ZonedDateTime.parse("2009-09-23T10:06:01-08:00", DateTimeFormatter.ISO_OFFSET_DATE_TIME),
                ZonedDateTime.parse("2009-09-23T10:06:02-08:00", DateTimeFormatter.ISO_OFFSET_DATE_TIME),
                ZonedDateTime.parse("2009-09-23T10:06:04-08:00", DateTimeFormatter.ISO_OFFSET_DATE_TIME));
        SortedSet<ProfileSample> result = ProfileDataConverter.splitDataIntoProfileSamples(profileConstituentList, readDateTimes, null, false, false);
        assertEquals(2, result.size());
        assertEquals(result.first().getDateTime(), ZonedDateTime.parse("2009-09-15T10:06:00-08:00", DateTimeFormatter.ISO_OFFSET_DATE_TIME));
        List<ProfileConstituent> firstDataList = result.first().getConstituents();
        ProfileConstituent depthConstituentResult = firstDataList.get(0);
        assertEquals("Depth", depthConstituentResult.getParameter());
        assertEquals(Arrays.asList(0.1, 5.0, 10.0, 15.0), depthConstituentResult.getDataValues());
        assertEquals("ft", depthConstituentResult.getUnit());
        ProfileConstituent tempConstituentResult = firstDataList.get(1);
        assertEquals("Temp-Water", tempConstituentResult.getParameter());
        assertEquals(Arrays.asList(10.0,11.0,12.0,13.0), tempConstituentResult.getDataValues());
        assertEquals("K", tempConstituentResult.getUnit());

        List<ProfileConstituent> secondDataList = result.last().getConstituents();
        ProfileConstituent depthConstituentResult2 = secondDataList.get(0);
        assertEquals("Depth", depthConstituentResult2.getParameter());
        assertEquals(Arrays.asList(0.1, 5.1, 10.1, 15.1), depthConstituentResult2.getDataValues());
        assertEquals("ft", depthConstituentResult2.getUnit());
        ProfileConstituent tempConstituentResult2 = secondDataList.get(1);
        assertEquals("Temp-Water", tempConstituentResult2.getParameter());
        assertEquals(Arrays.asList(10.0,9.0,8.0,7.0), tempConstituentResult2.getDataValues());
        assertEquals("K", tempConstituentResult2.getUnit());
    }

    @Test
    void testIsSignificantChange()
    {
        ZonedDateTime time1 = ZonedDateTime.parse("2009-09-15T10:06:00-08:00", DateTimeFormatter.ISO_OFFSET_DATE_TIME);
        ZonedDateTime time2 = ZonedDateTime.parse("2009-09-15T10:07:00-08:00", DateTimeFormatter.ISO_OFFSET_DATE_TIME);
        ZonedDateTime time3 = ZonedDateTime.parse("2009-09-16T10:18:00-08:00", DateTimeFormatter.ISO_OFFSET_DATE_TIME);
        double depth1 = 10.0;
        double depth2 = 11.0;
        double depth0 = 0.0;
        double max = 10.0;
        double min = 0.0;
        boolean isSignificantChange = ProfileDataConverter.isDifferenceSignificantChange(time1, time2, depth1, depth2, max, min);
        assertFalse(isSignificantChange);
        isSignificantChange = ProfileDataConverter.isDifferenceSignificantChange(time1, time3, depth1, depth2, max, min);
        assertTrue(isSignificantChange);
        isSignificantChange = ProfileDataConverter.isDifferenceSignificantChange(time1, time2, depth2, depth0, max, min);
        assertTrue(isSignificantChange);
    }

}
