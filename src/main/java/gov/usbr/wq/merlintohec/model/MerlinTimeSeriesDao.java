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
import gov.usbr.wq.dataaccess.model.DataWrapper;
import gov.usbr.wq.dataaccess.model.MeasureWrapper;
import gov.usbr.wq.merlintohec.exceptions.MerlinAccessException;
import gov.usbr.wq.merlintohec.exceptions.MerlinDataException;
import gov.usbr.wq.merlintohec.exceptions.MerlinNoDataFoundException;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.toList;

/**
 * This is intended to be the main class
 * Created by Ryan Miles
 */
public class MerlinTimeSeriesDao extends MerlinDao
{

	public List<TimeSeriesIdentifier> retrieveTimeSeriesIds(LocationTemplate template) throws MerlinNoDataFoundException, MerlinAccessException
	{
		List<TimeSeriesIdentifier> output = new ArrayList<>();

		MerlinTimeSeriesDataAccess access = new MerlinTimeSeriesDataAccess();
		try
		{
			access.getMeasurementsByProfile(getAccessToken(), MerlinDataConverter.locationTemplateToProfile(template));
		}
		catch (IOException ex)
		{
			throw new MerlinAccessException(ex);
		}
		catch (HttpAccessException ex)
		{
			//This isn't quite right
			throw new MerlinNoDataFoundException(ex);
		}

		return output;
	}

	public List<TimeSeries> retrieveTimeSeries(TimeSeriesTemplate template) throws MerlinNoDataFoundException, MerlinAccessException, MerlinDataException
	{
		List<TimeSeries> output = new ArrayList<>();

		Instant startTime = Instant.ofEpochMilli(template.getStartTime());
		Instant endTime = Instant.ofEpochMilli(template.getEndTime());
		TimeSeriesIdentifier id = template.getTimeSeriesIdentifier();
		LocationTemplate locTemplate;

		MerlinLocationDao locDao = new MerlinLocationDao();
		locTemplate = locDao.retrieveLocationTemplate(id.getLocationId(), id.getOfficeId()
																			.orElse(null));

		List<TimeSeriesIdentifier> identifiers = retrieveTimeSeriesIds(locTemplate);
		List<TimeSeriesIdentifier> similarIds = identifiers.stream()
														   .filter(id::describesSameDataAs)
														   .collect(toList());
		if (similarIds.isEmpty())
		{
			throw new MerlinNoDataFoundException("Unable to find time series ids like " + id.getTimeSeriesId());
		}
		else if (similarIds.size() > 1)
		{
			String message = "Too many time series ids matching " + id.getTimeSeriesId() + System.lineSeparator();
			message += similarIds.stream()
								 .map(temp -> "\t" + temp.getTimeSeriesId())
								 .collect(Collectors.joining(System.lineSeparator()));
			throw new MerlinNoDataFoundException(message);
		}

		MerlinTimeSeriesDataAccess access = new MerlinTimeSeriesDataAccess();
		try
		{
			TimeSeriesIdentifier realId = similarIds.get(0);
			MeasureWrapper measure = MerlinDataConverter.timeSeriesIdToMeasure(realId);
			DataWrapper timeSeriesData = access.getEventsBySeries(getAccessToken(), measure, startTime, endTime);
			output.add(MerlinDataConverter.dataToTimeSeries(timeSeriesData, id.getOfficeId().orElse(null)));
		}
		catch (RuntimeException ex)
		{
			throw new MerlinDataException(ex);
		}
		catch (IOException ex)
		{
			throw new MerlinAccessException("Unable to access the merlin web services.", ex);
		}
		catch (HttpAccessException ex)
		{
			//This isn't quite right
			throw new MerlinNoDataFoundException(ex);
		}

		return output;
	}
}
