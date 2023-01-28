/*
 * Copyright 2021  Hydrologic Engineering Center (HEC).
 * United States Army Corps of Engineers
 * All Rights Reserved.  HEC PROPRIETARY/CONFIDENTIAL.
 * Source may not be released without written approval
 * from HEC
 */

package gov.usbr.wq.merlintohec.model;

import gov.usbr.wq.dataaccess.model.DataWrapper;
import gov.usbr.wq.dataaccess.model.EventWrapper;
import hec.heclib.dss.DSSPathname;
import hec.heclib.util.HecTime;
import hec.io.TimeSeriesContainer;
import hec.lang.Const;
import mil.army.usace.hec.metadata.Interval;
import mil.army.usace.hec.metadata.IntervalFactory;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.NavigableSet;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

/**
 * Utility class designed for converting merlin data to and from HEC Data and MetaData objects.
 * Created by Ryan Miles
 */
final class MerlinDataConverter
{
	private MerlinDataConverter()
	{
		throw new AssertionError("This utility class is not intended to be instantiated.");
	}

	static TimeSeriesContainer dataToTimeSeries(DataWrapper data)
	{
		if (data.getEvents().isEmpty())
		{
			return null;
		}


		TimeSeriesContainer output = new TimeSeriesContainer();
		DSSPathname pathname = new DSSPathname(data.getSeriesId());
		String fPart = pathname.getFPart();
		int parsedInterval = Integer.parseInt(data.getTimestep());
		Optional<Interval> opt = IntervalFactory.findAny(interval -> interval.isRegular() && interval.getMinutes() == parsedInterval);
		String path = "/" + data.getProject() + "/" + data.getStation() + "-" + data.getSensor() + "/" +
				data.getParameter() + "//" +opt.map(Interval::getInterval).orElse("") + "/" + fPart;
		output.fullName = path;
		output.timeZoneID = data.getTimeZone().getId();
		output.units = data.getUnits();
		output.interval = parsedInterval;
		output.type = data.getDataType();
		output.parameter = data.getParameter();
		output.location = pathname.bPart();
		output.version = pathname.fPart();

		NavigableSet<EventWrapper> events = data.getEvents();

		int[] times = new int[events.size()];
		double[] values = new double[events.size()];
		int i = 0;

		for (EventWrapper event : events)
		{
			HecTime hecTime = fromZonedDateTime(event.getDate());
			int time = hecTime.value();
			times[i] = time;
			double value = Const.UNDEFINED_DOUBLE;
			if (event.getValue() != null)
			{
				value = event.getValue();
			}
			values[i] = value;

			i++;
		}

		output.values = values;
		output.times = times;
		output.numberValues = values.length;

		HecTime startTime = fromZonedDateTime(data.getStartTime());
		HecTime endTime = fromZonedDateTime(data.getEndTime());
		output.startTime = times[0];
		output.startHecTime = startTime;
		output.endTime = times[times.length - 1];
		output.endHecTime = endTime;

		return output;
	}

	static HecTime fromZonedDateTime(ZonedDateTime zonedDateTime)
	{
		//This comes from HecJavaDev v6.1, we should use that utility instead of this one if we ever upgrade.
		Instant instant = zonedDateTime.toInstant();
		ZoneId zoneId = zonedDateTime.getZone();
		HecTime hecTime = new HecTime();
		int offsetMinutes = (int) TimeUnit.SECONDS.toMinutes(zoneId.getRules().getOffset(instant).getTotalSeconds());
		hecTime.setTimeInMillis(instant.toEpochMilli(), offsetMinutes);
		return hecTime;
	}
}
