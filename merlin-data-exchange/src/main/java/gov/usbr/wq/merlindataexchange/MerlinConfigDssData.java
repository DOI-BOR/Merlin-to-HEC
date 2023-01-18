/*
 * Copyright {year} United States Bureau of Reclamation (USBR).
 * United States Department of the Interior
 * All Rights Reserved. USBR PROPRIETARY/CONFIDENTIAL.
 * Source may not be released without written approval
 * from USBR
 */

package gov.usbr.wq.merlindataexchange;

import java.util.ArrayList;
import java.util.List;

/**
 * This class represents one DSS file's collection of data.
 *
 * Created by Ryan Miles
 */
public class MerlinConfigDssData
{
	private final List<MerlinConfigData> _configData = new ArrayList<>();
	private final String _dssFilePath;
	private final boolean _mustExist;

	public MerlinConfigDssData(String dssFilePath, boolean mustExist)
	{
		_dssFilePath = dssFilePath;
		_mustExist = mustExist;
	}

	public String getDssFilePath()
	{
		return _dssFilePath;
	}

	public boolean isMustExist()
	{
		return _mustExist;
	}

	public List<MerlinConfigData> getConfigData()
	{
		return _configData;
	}
}
