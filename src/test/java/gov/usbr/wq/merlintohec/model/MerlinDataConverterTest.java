package gov.usbr.wq.merlintohec.model;

import gov.usbr.wq.dataaccess.json.Data;
import gov.usbr.wq.dataaccess.json.Measure;
import gov.usbr.wq.dataaccess.json.Profile;
import gov.usbr.wq.dataaccess.model.DataWrapper;
import gov.usbr.wq.dataaccess.model.MeasureWrapper;
import gov.usbr.wq.dataaccess.model.ProfileWrapper;
import mil.army.usace.hec.metadata.OfficeId;
import mil.army.usace.hec.metadata.location.LocationTemplate;
import mil.army.usace.hec.metadata.timeseries.TimeSeriesIdentifier;
import mil.army.usace.hec.metadata.timeseries.TimeSeriesIdentifierFactory;
import org.junit.jupiter.api.Test;

import java.time.ZoneId;

import static org.junit.jupiter.api.Assertions.*;

class MerlinDataConverterTest
{
	private static final String TEST_LOCATION = "Shasta Lake-Shasta Dam-Outflow";
	private static final String TEST_OFFICE_ID = "SWT";
	private static final String TEST_TIMESERIES_ID = "Shasta Lake-Shasta Dam-Outflow/Flow/INST-VAL/60/0/35-230.11.125.1.1";

	@Test
	void profileToLocationTemplate()
	{
		ProfileWrapper wrapper = new ProfileWrapper(new Profile().dprName(TEST_LOCATION));
		LocationTemplate locTemplate = MerlinDataConverter.profileToLocationTemplate(wrapper, new OfficeId(TEST_OFFICE_ID));
		assertEquals(wrapper.getName(), locTemplate.getLocationId());
		assertEquals(TEST_OFFICE_ID, locTemplate.getOfficeId());
	}

	@Test
	void locationTemplateToProfile()
	{
		LocationTemplate loc = new LocationTemplate(TEST_OFFICE_ID, TEST_LOCATION);
		ProfileWrapper wrapper = MerlinDataConverter.locationTemplateToProfile(loc);
		assertEquals(wrapper.getName(), TEST_LOCATION);
		assertNull(wrapper.getDprId());
	}

	@Test
	void timeSeriesIdToMeasure() throws Exception
	{
		TimeSeriesIdentifier tsId = TimeSeriesIdentifierFactory.from(new OfficeId(TEST_OFFICE_ID), TEST_TIMESERIES_ID, ZoneId.of("PST"));
		MeasureWrapper measure = MerlinDataConverter.timeSeriesIdToMeasure(tsId);
		assertEquals(tsId.getTimeSeriesId(), measure.getSeriesString());
	}

	@Test
	void measureToTimeSeriesId() throws Exception
	{
		MeasureWrapper measure = new MeasureWrapper(new Measure().seriesString(TEST_TIMESERIES_ID));
		TimeSeriesIdentifier tsId = MerlinDataConverter.measureToTimeSeriesId(measure, new OfficeId(TEST_OFFICE_ID));
		assertEquals(measure.getSeriesString(), tsId.getTimeSeriesId());
	}

	@Test
	void dataWrapperToTimeSeries()
	{

	}
}