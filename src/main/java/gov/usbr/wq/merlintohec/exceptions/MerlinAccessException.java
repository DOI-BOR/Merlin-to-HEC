/*
 * Copyright 2021  Hydrologic Engineering Center (HEC).
 * United States Army Corps of Engineers
 * All Rights Reserved.  HEC PROPRIETARY/CONFIDENTIAL.
 * Source may not be released without written approval
 * from HEC
 */

package gov.usbr.wq.merlintohec.exceptions;

/**
 * Created by Ryan Miles
 */
public class MerlinAccessException extends MerlinException
{
	public MerlinAccessException(String message)
	{
		super(message);
	}

	public MerlinAccessException(String message, Throwable cause)
	{
		super(message, cause);
	}

	public MerlinAccessException(Throwable cause)
	{
		super(cause);
	}
}
