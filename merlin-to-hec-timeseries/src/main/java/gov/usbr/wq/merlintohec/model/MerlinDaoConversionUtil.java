/*
 * Copyright 2021  Hydrologic Engineering Center (HEC).
 * United States Army Corps of Engineers
 * All Rights Reserved.  HEC PROPRIETARY/CONFIDENTIAL.
 * Source may not be released without written approval
 * from HEC
 */

package gov.usbr.wq.merlintohec.model;

import gov.usbr.wq.dataaccess.model.DataWrapper;
import gov.usbr.wq.merlintohec.exceptions.MerlinInvalidTimestepException;
import hec.io.TimeSeriesContainer;
import hec.ui.ProgressListener;
import hec.ui.ProgressListener.MessageType;

/**
 * This is intended to be the main class
 */
public final class MerlinDaoConversionUtil
{
	private MerlinDaoConversionUtil()
	{
		throw new AssertionError("Utility Class should not be instantiated");
	}

	public static TimeSeriesContainer convertToTsc(DataWrapper data, String unitSystemToConvertTo, String fPartOverride, ProgressListener progressListener) throws MerlinInvalidTimestepException
	{
		TimeSeriesContainer output = null;
		if(data != null && data.getSeriesId() != null && !data.getSeriesId().isEmpty())
		{
			if(progressListener != null)
			{
				progressListener.progress("Converting data for " + data.getSeriesId() + " to timeseries...", MessageType.IMPORTANT);
			}
			output = MerlinDataConverter.dataToTimeSeries(data, unitSystemToConvertTo, fPartOverride, progressListener);
		}

		return output;
	}
}
