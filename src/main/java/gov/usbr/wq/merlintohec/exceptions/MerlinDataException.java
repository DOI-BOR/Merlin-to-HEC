/*
 * Copyright 2021  Hydrologic Engineering Center (HEC).
 * United States Army Corps of Engineers
 * All Rights Reserved.  HEC PROPRIETARY/CONFIDENTIAL.
 * Source may not be released without written approval
 * from HEC
 */

package gov.usbr.wq.merlintohec.exceptions;

/**
 * Exception that occurs when the data for Merlin objects do not line up with data required for HEC data objects.
 * Created by Ryan Miles
 */
public class MerlinDataException extends MerlinException
{
	public MerlinDataException(String message)
	{
		super(message);
	}

	public MerlinDataException(String message, Throwable cause)
	{
		super(message, cause);
	}

	public MerlinDataException(Throwable cause)
	{
		super(cause);
	}
}
