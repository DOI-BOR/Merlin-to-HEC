/*
 * Copyright 2021  Hydrologic Engineering Center (HEC).
 * United States Army Corps of Engineers
 * All Rights Reserved.  HEC PROPRIETARY/CONFIDENTIAL.
 * Source may not be released without written approval
 * from HEC
 */

package gov.usbr.wq.merlintohec.model;

import gov.usbr.wq.dataaccess.http.HttpAccess;
import gov.usbr.wq.dataaccess.http.HttpAccessUtils;
import gov.usbr.wq.dataaccess.jwt.JwtContainer;
import gov.usbr.wq.merlintohec.exceptions.MerlinAccessException;

import java.io.IOException;

/**
 * Created by Ryan Miles
 */
public abstract class MerlinDao
{



	private static String getUsername()
	{
		//Not sure if this is the appropriate way to do this.
		return System.getProperty("merlin.username", "webserviceuser");
	}

	private static String getPassword()
	{
		//Not sure if this is the appropriate way to do this.
		return System.getProperty("merlin.password", "T3stUser!");
	}

	protected static JwtContainer getAccessToken() throws MerlinAccessException
	{
		String username = getUsername();
		String password = getPassword();
		try
		{
			return (JwtContainer)HttpAccessUtils.authenticate(username, password);
		}
		catch (IOException ex)
		{
			throw new MerlinAccessException("Unable to authenticate with the merlin web services.", ex);
		}
	}
}
