/*
 * Copyright 2021  Hydrologic Engineering Center (HEC).
 * United States Army Corps of Engineers
 * All Rights Reserved.  HEC PROPRIETARY/CONFIDENTIAL.
 * Source may not be released without written approval
 * from HEC
 */

package gov.usbr.wq.merlintohec.model;

import gov.usbr.wq.dataaccess.http.HttpAccess;
import gov.usbr.wq.dataaccess.jwt.JwtContainer;

import java.io.IOException;

/**
 * Created by Ryan Miles
 */
public abstract class MerlinDao
{



	private static String getUsername()
	{
		//Not sure if this is the appropriate way to do this.
		return System.getProperty("merlin-username");
	}

	private static String getPassword()
	{
		//Not sure if this is the appropriate way to do this.
		return System.getProperty("merlin-password");
	}

	protected static JwtContainer getAccessToken() throws IOException
	{
		String username = getUsername();
		String password = getPassword();
		return new HttpAccess(HttpAccess.getDefaultWebServiceRoot()).authenticate(username, password);
	}
}
