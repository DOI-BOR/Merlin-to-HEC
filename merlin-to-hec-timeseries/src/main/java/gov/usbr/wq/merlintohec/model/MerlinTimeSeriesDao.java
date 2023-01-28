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
import gov.usbr.wq.dataaccess.jwt.TokenContainer;
import gov.usbr.wq.dataaccess.model.DataWrapper;
import gov.usbr.wq.dataaccess.model.MeasureWrapper;
import gov.usbr.wq.dataaccess.model.TemplateWrapper;
import gov.usbr.wq.merlintohec.exceptions.MerlinAccessException;
import hec.io.TimeSeriesContainer;

import java.io.IOException;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Collection;
import java.util.logging.Level;
import java.util.logging.Logger;
import static java.util.stream.Collectors.toList;

/**
 * This is intended to be the main class
 */
public final class MerlinTimeSeriesDao
{
	private static final Logger LOGGER = Logger.getLogger(MerlinTimeSeriesDao.class.getName());


	public List<TimeSeriesContainer> catalogTimeSeries(Instant startTime, Instant endTime, Integer qualityVersionId, TokenContainer accessToken) throws MerlinAccessException
	{
		MerlinTimeSeriesDataAccess access = new MerlinTimeSeriesDataAccess();

		return retrieveTemplates(accessToken, access).stream()
													.map(template -> retrieveMeasures(accessToken, access, template))
													.flatMap(Collection::stream)
													.map(measure -> retrieveData(accessToken, access, measure, qualityVersionId, startTime, endTime))
													.filter(Objects::nonNull)
													.map(this::convertToTsc)
													.filter(Objects::nonNull)
													.collect(toList());
	}

	private TimeSeriesContainer convertToTsc(DataWrapper data)
	{
		TimeSeriesContainer output = null;
		if(data.getSeriesId() != null && !data.getSeriesId().isEmpty())
		{
			output = MerlinDataConverter.dataToTimeSeries(data);
		}

		return output;
	}

	private List<TemplateWrapper> retrieveTemplates(TokenContainer accessToken, MerlinTimeSeriesDataAccess access) throws MerlinAccessException
	{
		try
		{
			return access.getTemplates(accessToken);
		}
		catch (IOException | HttpAccessException ex)
		{
			throw new MerlinAccessException("Unable to retrieve templates.", ex);
		}
	}

	private List<MeasureWrapper> retrieveMeasures(TokenContainer accessToken, MerlinTimeSeriesDataAccess access, TemplateWrapper template)
	{
		try
		{
			return access.getMeasurementsByTemplate(accessToken, template);
		}
		catch (HttpAccessException | IOException ex)
		{
			LOGGER.log(Level.WARNING, ex, () -> "Unable to access the merlin web services to retrieve measurements for template " + template);
			return Collections.emptyList();
		}
	}

	private DataWrapper retrieveData(TokenContainer accessToken, MerlinTimeSeriesDataAccess access, MeasureWrapper measure,
									 Integer qualityVersionId, Instant startTime, Instant endTime)
	{
		if (startTime == null)
		{
			startTime = Instant.ofEpochMilli(Long.MIN_VALUE);
		}
		if (endTime == null)
		{
			endTime =  Instant.ofEpochMilli(Long.MAX_VALUE);
		}

		DataWrapper output = null;
		try
		{
			output = access.getEventsBySeries(accessToken, measure, qualityVersionId, startTime, endTime);
		}
		catch (HttpAccessException | IOException ex)
		{
			LOGGER.log(Level.WARNING, ex, () -> "Unable to access the merlin web services to retrieve measurements for " + measure);
		}
		return output;
	}
}
