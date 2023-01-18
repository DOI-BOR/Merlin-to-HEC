/*
 * Copyright 2021  Hydrologic Engineering Center (HEC).
 * United States Army Corps of Engineers
 * All Rights Reserved.  HEC PROPRIETARY/CONFIDENTIAL.
 * Source may not be released without written approval
 * from HEC
 */

package gov.usbr.wq.merlintohec.model;

import gov.usbr.wq.dataaccess.MerlinTimeSeriesDataAccess;
import gov.usbr.wq.dataaccess.http.HttpAccessException;
import gov.usbr.wq.dataaccess.jwt.JwtContainer;
import gov.usbr.wq.dataaccess.model.DataWrapper;
import gov.usbr.wq.dataaccess.model.MeasureWrapper;
import gov.usbr.wq.dataaccess.model.ProfileWrapper;
import gov.usbr.wq.merlintohec.exceptions.MerlinAccessException;
import gov.usbr.wq.merlintohec.exceptions.MerlinDataException;
import hec.io.TimeSeriesContainer;

import java.io.IOException;
import java.time.Instant;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.logging.Level;
import java.util.logging.Logger;

import static java.util.stream.Collectors.toList;

/**
 * This is intended to be the main class
 * Created by Ryan Miles
 */
public class MerlinTimeSeriesDao extends MerlinDao
{
	private static final Logger LOGGER = Logger.getLogger(MerlinTimeSeriesDao.class.getName());

	public List<TimeSeriesContainer> catalogTimeSeries() throws MerlinAccessException
	{
		return catalogTimeSeries(null, null);
	}

	public List<TimeSeriesContainer> catalogTimeSeries(Instant startTime, Instant endTime) throws MerlinAccessException
	{
		JwtContainer accessToken = getAccessToken();
		MerlinTimeSeriesDataAccess access = new MerlinTimeSeriesDataAccess();

		return retrieveProfiles(accessToken, access).stream()
													.map(profile -> retrieveMeasures(accessToken, access, profile))
													.flatMap(Collection::stream)
													.map(measure -> retrieveData(accessToken, access, measure, startTime, endTime))
													.filter(Objects::nonNull)
													.map(this::convertToTsc)
													.filter(Objects::nonNull)
													.collect(toList());
	}

	private TimeSeriesContainer convertToTsc(DataWrapper data)
	{
		TimeSeriesContainer output = null;
		try
		{
			output = MerlinDataConverter.dataToTimeSeries(data);
		}
		catch (MerlinDataException ex)
		{
			//Not sure what to do in this case yet.
		}

		return output;
	}

	private List<ProfileWrapper> retrieveProfiles(JwtContainer accessToken, MerlinTimeSeriesDataAccess access) throws MerlinAccessException
	{
		try
		{
			return access.getProfiles(accessToken);
		}
		catch (IOException | HttpAccessException ex)
		{
			throw new MerlinAccessException("Unable to retrieve profiles.", ex);
		}
	}

	private List<MeasureWrapper> retrieveMeasures(JwtContainer accessToken, MerlinTimeSeriesDataAccess access, ProfileWrapper profile)
	{
		try
		{
			return access.getMeasurementsByProfile(accessToken, profile);
		}
		catch (HttpAccessException | IOException ex)
		{
			LOGGER.log(Level.WARNING, "Unable to access the merlin web services to retrieve measurements for profile " + profile, ex);
			return Collections.emptyList();
		}
	}

	private DataWrapper retrieveData(JwtContainer accessToken, MerlinTimeSeriesDataAccess access, MeasureWrapper measure, Instant startTime, Instant endTime)
	{
		if (startTime == null)
		{
			startTime = Instant.MIN;
		}
		if (endTime == null)
		{
			endTime = Instant.MAX;
		}

		DataWrapper output = null;
		try
		{
			output = access.getEventsBySeries(accessToken, measure, startTime, endTime);
		}
		catch (HttpAccessException | IOException ex)
		{
			LOGGER.log(Level.WARNING, "Unable to access the merlin web services to retrieve measurements for " + measure, ex);
		}
		return output;
	}
}
