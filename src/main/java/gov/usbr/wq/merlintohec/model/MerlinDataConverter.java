/*
 * Copyright 2021  Hydrologic Engineering Center (HEC).
 * United States Army Corps of Engineers
 * All Rights Reserved.  HEC PROPRIETARY/CONFIDENTIAL.
 * Source may not be released without written approval
 * from HEC
 */

package gov.usbr.wq.merlintohec.model;

import gov.usbr.wq.dataaccess.json.Measure;
import gov.usbr.wq.dataaccess.json.Profile;
import gov.usbr.wq.dataaccess.model.DataWrapper;
import gov.usbr.wq.dataaccess.model.EventWrapper;
import gov.usbr.wq.dataaccess.model.MeasureWrapper;
import gov.usbr.wq.dataaccess.model.ProfileWrapper;
import gov.usbr.wq.merlintohec.exceptions.MerlinDataException;
import hec.data.DataSetException;
import hec.data.DataSetIllegalArgumentException;
import hec.data.Units;
import hec.data.location.LocationTemplate;
import hec.data.tx.DescriptionTx;
import hec.io.TimeSeriesContainer;
import hec.lang.Const;
import hec.lang.DSSPathString;

import java.time.ZoneId;
import java.time.ZonedDateTime;

/**
 * Utility class designed for converting merlin data to and from HEC Data and MetaData objects.
 *
 * Created by Ryan Miles
 */
final class MerlinDataConverter
{
	private MerlinDataConverter()
	{
		throw new AssertionError("This utility class is not intended to be instantiated.");
	}

	public static LocationTemplate profileToLocationTemplate(ProfileWrapper profileWrapper, String officeId)
	{
		return new LocationTemplate(officeId, profileWrapper.getName());
	}

	public static ProfileWrapper locationTemplateToProfile(LocationTemplate template)
	{
		Profile profile = new Profile().dprName(template.getLocationId());
		return new ProfileWrapper(profile);
	}

	public static MeasureWrapper timeSeriesIdToMeasure(DescriptionTx id)
	{
		Measure measure = new Measure().seriesString(id.getTimeSeriesId());
		return new MeasureWrapper(measure);
	}

	public static DSSPathString measureToTimeSeriesId(MeasureWrapper wrapper, String officeId) throws DataSetIllegalArgumentException
	{
		return new DSSPathString(wrapper.getSeriesString());
	}

	public static TimeSeriesContainer dataToTimeSeries(DataWrapper data, String officeId) throws MerlinDataException
	{
		try
		{
			ZoneId timeZone = data.getTimeZone();
			DSSPathString id = new DSSPathString(data.getSeriesId());
			Units units = new Units(data.getUnits());

			long startMillis = ZonedDateTime.of(data.getStartTime(), timeZone).toInstant().toEpochMilli();
			long endMillis = ZonedDateTime.of(data.getEndTime(), timeZone).toInstant().toEpochMilli();

			int eventCount = data.getEvents().size();
			long[] times = new long[eventCount];
			double[] values = new double[eventCount];
			int[] qualities = new int[eventCount];
			boolean makeQuality = false;
			int i = 0;

			for (EventWrapper event : data.getEvents())
			{
				times[i] = ZonedDateTime.of(event.getDate(), timeZone).toInstant().toEpochMilli();
				Double value = event.getValue();
				if (value == null)
				{
					value = Const.UNDEFINED_DOUBLE;
				}
				values[i] = value;
				Integer quality = event.getQuality();
				if (quality == null)
				{
					makeQuality = true;
					quality = 0;
				}
				else
				{

				}
				qualities[i] = quality;
			}

			Quality quality = null;

			if (makeQuality)
			{
				quality = new Quality(qualities);
			}

			return TimeSeriesFactory.buildTimeSeries(template, times, values, quality);
		}
		catch (DataSetException ex)
		{
			throw new MerlinDataException(ex);
		}
	}
}
