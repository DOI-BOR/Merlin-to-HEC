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
public abstract class MerlinException extends Exception
{
	public MerlinException(String message)
	{
		super(message);
	}

	public MerlinException(String message, Throwable cause)
	{
		super(message, cause);
	}

	public MerlinException(Throwable cause)
	{
		super(cause);
	}
}
