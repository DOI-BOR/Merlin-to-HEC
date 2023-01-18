package gov.usbr.wq.merlintohec.model;

import gov.usbr.wq.merlintohec.exceptions.MerlinAccessException;
import hec.io.TimeSeriesContainer;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class MerlinTimeSeriesDaoTest
{

//	@Test
	void catalogTimeSeries() throws MerlinAccessException
	{
		MerlinTimeSeriesDao dao = new MerlinTimeSeriesDao();
		List<TimeSeriesContainer> containers = dao.catalogTimeSeries();
		assertFalse(containers.isEmpty());
	}
}