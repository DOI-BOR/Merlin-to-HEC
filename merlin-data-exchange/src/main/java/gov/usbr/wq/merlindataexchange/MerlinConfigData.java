/*
 * Copyright {year} United States Bureau of Reclamation (USBR).
 * United States Department of the Interior
 * All Rights Reserved. USBR PROPRIETARY/CONFIDENTIAL.
 * Source may not be released without written approval
 * from USBR
 */

package gov.usbr.wq.merlindataexchange;

/**
 * Created by Ryan Miles
 */
public class MerlinConfigData
{
	private final String _merlinId;		//Merlin id
	private final String _dssFilePath;	//Needs to support relative pathing, but what is it relative to?
	private final String _dssPathname;	//Pathname to store.
	private final boolean _mustExist;

	public MerlinConfigData(String merlinId, String dssFilePath, String dssPathname, boolean mustExist)
	{
		_merlinId = merlinId;
		_dssFilePath = dssFilePath;
		_dssPathname = dssPathname;
		_mustExist = mustExist;
	}

	public String getMerlinId()
	{
		return _merlinId;
	}

	public String getDssFilePath()
	{
		return _dssFilePath;
	}

	public String getDssPathname()
	{
		return _dssPathname;
	}

	public boolean isMustExist()
	{
		return _mustExist;
	}
}
