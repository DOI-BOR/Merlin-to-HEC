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
import gov.usbr.wq.merlintohec.exceptions.MerlinInvalidTimestepException;
import hec.data.Units;
import hec.data.UnitsConversionException;
import hec.heclib.dss.DSSPathname;
import hec.heclib.dss.HecTimeSeriesBase;
import hec.heclib.util.HecTime;
import hec.heclib.util.Unit;
import hec.io.TimeSeriesContainer;
import hec.lang.Const;
import hec.ui.ProgressListener;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.NavigableSet;
import java.util.concurrent.TimeUnit;
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

	static TimeSeriesContainer dataToTimeSeries(DataWrapper data, String unitSystemToConvertTo, String fPartOverride, ProgressListener progressListener, Logger logFileLogger)
			throws MerlinInvalidTimestepException
	{
		TimeSeriesContainer output = new TimeSeriesContainer();
		if (!data.getEvents().isEmpty())
		{
			String timeStep = data.getTimestep();
			if (timeStep == null || timeStep.contains(","))
			{
				throw new MerlinInvalidTimestepException(timeStep, data.getSeriesId());
			}

			DSSPathname pathname = new DSSPathname(data.getSeriesId());
			pathname.setAPart(data.getProject());
			pathname.setBPart(data.getStation() + "-" + data.getSensor());
			pathname.setCPart(data.getParameter());
			String fPart = pathname.getFPart();

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
			output.fullName = pathname.getPathname();
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
			try
			{
				convertUnits(output, unitSystemToConvertTo, data, progressListener, logFileLogger);
			}
			catch (UnitsConversionException e)
			{
				logUnitConversionError(e, logFileLogger, progressListener);
			}
		}
		return output;
	}

	private static void logUnitConversionError(Exception e, Logger logFileLogger, ProgressListener progressListener)
	{
		logFileLogger.log(Level.SEVERE, e, () -> "Failed to determine units to convert to");
		LOGGER.log(Level.CONFIG, e, () -> "Failed to determine units to convert to");
		if (progressListener != null)
		{
			progressListener.progress("Failed to determine units to convert to", ProgressListener.MessageType.ERROR);
		}
	}


	private static void convertUnits(TimeSeriesContainer output, String unitSystemToConvertTo, DataWrapper data, ProgressListener progressListener, Logger logFileLogger)
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
		String unitsTo = Units.getUnitsInUnitSystem(unitSystemToConvertTo, data.getUnits());
		String unitsFrom = data.getUnits();
		if (!unitsFrom.equalsIgnoreCase(unitsTo))
		{
			if (progressListener != null)
			{
				progressListener.progress("Converting units from " + unitsFrom + " to " + unitsTo);
			}
			logFileLogger.info(() -> "Converting units from " + unitsFrom + " to " + unitsTo);
			Units.convertUnits(output, convertToUnitSystemId);
		}
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
