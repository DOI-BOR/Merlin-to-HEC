/*
 * Copyright 2021  Hydrologic Engineering Center (HEC).
 * United States Army Corps of Engineers
 * All Rights Reserved.  HEC PROPRIETARY/CONFIDENTIAL.
 * Source may not be released without written approval
 * from HEC
 */

package gov.usbr.wq.merlindataexchange.io;

import gov.usbr.wq.dataaccess.model.DataWrapper;
import gov.usbr.wq.dataaccess.model.EventWrapper;
import gov.usbr.wq.merlindataexchange.NoEventsException;
import hec.data.DataSetIllegalArgumentException;
import hec.data.Interval;
import hec.data.IntervalOffset;
import hec.data.Units;
import hec.data.UnitsConversionException;
import hec.heclib.dss.DSSPathname;
import hec.heclib.dss.HecTimeSeriesBase;
import hec.heclib.util.HecTime;
import hec.heclib.util.Unit;
import hec.hecmath.HecMath;
import hec.hecmath.HecMathException;
import hec.hecmath.TimeSeriesMath;
import hec.io.DataContainer;
import hec.io.TimeSeriesContainer;
import hec.lang.Const;
import hec.ui.ProgressListener;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Date;
import java.util.NavigableSet;
import java.util.TimeZone;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Utility class designed for converting merlin data to and from HEC Data and MetaData objects.
 * Created by Ryan Miles
 */
final class MerlinDataConverter
{

	private static final Logger LOGGER = Logger.getLogger(MerlinDataConverter.class.getName());
	private MerlinDataConverter()
	{
		throw new AssertionError("This utility class is not intended to be instantiated.");
	}

	static TimeSeriesContainer dataToTimeSeries(DataWrapper data, String unitSystemToConvertTo, String fPartOverride, boolean isProcessed, ProgressListener progressListener)
			throws MerlinInvalidTimestepException, NoEventsException, HecMathException, DataSetIllegalArgumentException
	{
		TimeSeriesContainer output = new TimeSeriesContainer();
		if(data != null && data.getSeriesId() != null && !data.getSeriesId().isEmpty())
		{
			String timeStep = data.getTimestep();
			if (timeStep == null || timeStep.contains(","))
			{
				throw new MerlinInvalidTimestepException(timeStep, data.getSeriesId());
			}

			DSSPathname pathname = new DSSPathname(data.getSeriesId());
			pathname.setAPart(data.getProject());
			pathname.setBPart(data.getStation() + "-" + data.getMeasurement());
			pathname.setCPart(data.getParameter());
			String[] seriesSplit = data.getSeriesId().split("/");
			String fPart = seriesSplit[seriesSplit.length - 1];
			int parsedInterval = Integer.parseInt(data.getTimestep());
			String interval = HecTimeSeriesBase.getEPartFromInterval(parsedInterval);
			pathname.setFPart(fPart);
			if (fPartOverride != null)
			{
				pathname.setFPart(fPartOverride);
			}
			pathname._delimiter = "/";
			pathname.setEPart("" + interval);
			pathname.setDPart("");
			if(data.getEvents().isEmpty())
			{
				throw new NoEventsException(pathname.getPathname(), data.getSeriesId());
			}
			output.fullName = pathname.getPathname();
			ZoneId dataZoneId = data.getTimeZone();
			output.setTimeZoneID(dataZoneId.getId());
			output.locationTimezone = dataZoneId.getId();
			output.units = data.getUnits();
			output.interval = parsedInterval;
			output.type = data.getDataType();
			output.parameter = data.getParameter();
			output.location = pathname.bPart();
			output.version = pathname.fPart();
			output.setStoreAsDoubles(true);

			NavigableSet<EventWrapper> events = data.getEvents();
			int[] times = new int[events.size()];
			double[] values = new double[events.size()];
			int i = 0;
			boolean needsInterpolation = isInterpolationNeeded(isProcessed, data.getStartTime(), data.getEndTime(), data.getTimestep(), dataZoneId, events.size());
			for (EventWrapper event : events)
			{
				HecTime hecTime = fromZonedDateTime(event.getDate(), dataZoneId);
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

			HecTime startTime = fromZonedDateTime(data.getStartTime(), dataZoneId);
			HecTime endTime = fromZonedDateTime(data.getEndTime(), dataZoneId);
			output.startTime = times[0];
			output.startHecTime = startTime;
			output.endTime = times[times.length - 1];
			output.endHecTime = endTime;
			try
			{
				if(needsInterpolation)
				{
					output.interval = HecTimeSeriesBase.getIntervalFromEPart("IR-MONTH");
					int offsetInMinutes = calculateOffsetInMinutes(data.getStartTime(), new Interval(parsedInterval), TimeZone.getTimeZone(dataZoneId));
					output = interpolateTimeSeries(output, parsedInterval, offsetInMinutes);
					output.startTime = output.times[0];
					output.startHecTime = startTime;
					output.endTime = output.times[output.times.length - 1];
					output.endHecTime = endTime;
					output.locationTimezone = dataZoneId.getId();
				}
				convertUnits(output, unitSystemToConvertTo, data);
			}
			catch (UnitsConversionException e)
			{
				logUnitConversionError(e, progressListener);
			}

		}
		return output;
	}

	private static boolean isInterpolationNeeded(boolean isProcessed, ZonedDateTime startTime, ZonedDateTime endTime, String timeStep, ZoneId dataZoneId, int numberOfEvents)
			throws DataSetIllegalArgumentException
	{
		boolean retVal = !isProcessed;
		if(!isProcessed)
		{
			int parsedInterval = Integer.parseInt(timeStep);
			int offsetMinutes = calculateOffsetInMinutes(startTime, new Interval(parsedInterval), TimeZone.getTimeZone(dataZoneId));
			int numIntervals = calculateNumberOfIntervals(startTime, endTime, offsetMinutes, parsedInterval, dataZoneId);
			retVal = numIntervals + 1 != numberOfEvents;
		}
		return retVal;
	}

	private static int calculateNumberOfIntervals(ZonedDateTime startTime, ZonedDateTime endTime, int offsetMinutes, int parsedInterval, ZoneId dataZoneId)
			throws DataSetIllegalArgumentException
	{
		IntervalOffset offset = new IntervalOffset(offsetMinutes*60, parsedInterval*60);
		return (int) Interval.calcNumberOfIntervals(Date.from(startTime.toInstant()),
				Date.from(endTime.toInstant()), new Interval(parsedInterval), offset, TimeZone.getTimeZone(dataZoneId));
	}

	private static TimeSeriesContainer interpolateTimeSeries(TimeSeriesContainer output, int interval, int offsetInMinutes) throws HecMathException
	{
		TimeSeriesMath timeSeriesMath = new TimeSeriesMath(output);
		HecMath hecMath = timeSeriesMath.interpolateDataAtRegularInterval(interval + "M", offsetInMinutes + "M");
		if (hecMath instanceof TimeSeriesMath)
		{
			DataContainer dataContainer = hecMath.getData();
			if (dataContainer instanceof TimeSeriesContainer)
			{
				output = (TimeSeriesContainer) dataContainer;
			}
		}
		output.setStoreAsDoubles(true);
		return output;
	}

	private static int calculateOffsetInMinutes(ZonedDateTime start, Interval interval, TimeZone timeZone) throws DataSetIllegalArgumentException
	{
		Instant prevInterval = Instant.ofEpochMilli(Interval.getPreviousIntervalTime(start.toInstant().toEpochMilli(), interval, timeZone));
		Instant interValToCheck = Instant.ofEpochMilli(Interval.getNextIntervalTime(prevInterval.toEpochMilli(), interval, timeZone));
		return (int) Duration.between(interValToCheck, start.toInstant()).toMinutes();
	}

	private static void logUnitConversionError(Exception e, ProgressListener progressListener)
	{
		LOGGER.log(Level.CONFIG, e, () -> "Failed to determine units to convert to");
		if (progressListener != null)
		{
			progressListener.progress("Failed to determine units to convert to", ProgressListener.MessageType.ERROR);
		}
	}


	private static void convertUnits(TimeSeriesContainer output, String unitSystemToConvertTo, DataWrapper data)
			throws UnitsConversionException
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
		Units.convertUnits(output, convertToUnitSystemId);
	}

	static HecTime fromZonedDateTime(ZonedDateTime zonedDateTime, ZoneId zoneIdToConvertTo)
	{
		zonedDateTime = ZonedDateTime.ofInstant(zonedDateTime.toInstant(), zoneIdToConvertTo);
		return HecTime.fromZonedDateTime(zonedDateTime);
	}

}
