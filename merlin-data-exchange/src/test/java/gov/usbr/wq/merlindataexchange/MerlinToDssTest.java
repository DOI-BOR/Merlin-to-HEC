/*
 * Copyright {year} United States Bureau of Reclamation (USBR).
 * United States Department of the Interior
 * All Rights Reserved. USBR PROPRIETARY/CONFIDENTIAL.
 * Source may not be released without written approval
 * from USBR
 */

package gov.usbr.wq.merlindataexchange;

import gov.usbr.wq.merlintohec.model.MerlinTimeSeriesDao;
import hec.heclib.dss.HecTimeSeries;
import hec.heclib.util.Heclib;
import hec.io.TimeSeriesContainer;
import org.junit.jupiter.api.Test;

import java.nio.file.Paths;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Created by Ryan Miles
 */
public class MerlinToDssTest
{

	private static final Logger LOGGER = Logger.getLogger(MerlinToDssTest.class.getName());

	@Test
	public void catalogToDss() throws Exception
	{
		MerlinTimeSeriesDao dao = new MerlinTimeSeriesDao();
		Instant endTime = Instant.now();
		Instant startTime = endTime.minus(100, ChronoUnit.DAYS);
		List<TimeSeriesContainer> ts = dao.catalogTimeSeries();

		String file = Paths.get("merlin_folsom.dss").toAbsolutePath().toString();
		ts.forEach(tsc -> tsc.fileName = file);
		LOGGER.log(Level.SEVERE, file);

		HecTimeSeries dssStor = new HecTimeSeries();
		try
		{
			dssStor.setDSSFileName(file);
			for (TimeSeriesContainer tsc : ts)
			{
				{
					int err = dssStor.write(tsc);
					if (err != 0)
					{
						LOGGER.log(Level.SEVERE, "Error saving " + tsc.fullName + ".  Code: " + err);
					}
				}
			}
			Heclib.squeezeDSS(file);
		}
		finally
		{
			dssStor.done();
		}
	}
}
