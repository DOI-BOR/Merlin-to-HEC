/*
 * Copyright 2022 United States Bureau of Reclamation (USBR).
 * United States Department of the Interior
 * All Rights Reserved. USBR PROPRIETARY/CONFIDENTIAL.
 * Source may not be released without written approval
 * from USBR
 */

package gov.usbr.wq.merlindataexchange;

import gov.usbr.wq.merlintohec.exceptions.MerlinAccessException;
import gov.usbr.wq.merlintohec.model.MerlinTimeSeriesDao;
import hec.heclib.dss.HecDss;
import hec.io.TimeSeriesContainer;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Primary class for the data exchange between merlin and DSS.
 * Created by Ryan Miles
 */
public class MerlinDataExchange
{
	private static final Logger LOGGER = Logger.getLogger(MerlinDataExchange.class.getName());

	public MerlinDataExchange()
	{
		//Empty ctor
	}

	public void exchange(Path configFilePath) throws IOException, MerlinAccessException
	{
		MerlinConfigFile configFile = new MerlinConfigFile();
		List<MerlinConfigDssData> configData = configFile.readConfigFile(configFilePath);

		MerlinTimeSeriesDao dao = new MerlinTimeSeriesDao();

		//Need to set the username and password for the dao somehow.  I probably need to read it in somehow?
//		List<TimeSeriesContainer> catalog = dao.catalogTimeSeries();
//
//		for (MerlinConfigDssData dssData : configData)
//		{
//			String dssFilePath = dssData.getDssFilePath();
//			boolean mustExist = dssData.isMustExist();
//
//			try
//			{
//				HecDss dss = HecDss.open(dssFilePath, mustExist);
//
//				for (MerlinConfigData data : dssData.getConfigData())
//				{
//					String merlinId = data.getMerlinId();
//					catalog.stream()
//						   .filter(tsc -> tsc.fullName.equalsIgnoreCase(merlinId))
//						   .findFirst()
//						   .map(tsc -> updateTsc(tsc, data))
//						   .ifPresent(tsc -> storeTsc(tsc, dss));
//				}
//			}
//			catch (Exception ex)
//			{
//				throw new IOException("Unable to access DSS file: " + dssFilePath, ex);
//			}
//		}
	}

	private void storeTsc(TimeSeriesContainer tsc, HecDss dss)
	{
		try
		{
			dss.save(tsc);
		}
		catch (Exception ex)
		{
			LOGGER.log(Level.SEVERE, ex, () -> "Unable to store time series data for " + tsc.fullName);
		}
	}

	private TimeSeriesContainer updateTsc(TimeSeriesContainer tsc, MerlinConfigData data)
	{
		TimeSeriesContainer output = (TimeSeriesContainer) tsc.clone();
		output.fileName = data.getDssFilePath();

		//tsc should have everything it needs to know the interval and parameter and such.


		return output;
	}
}
