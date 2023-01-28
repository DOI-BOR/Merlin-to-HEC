package gov.usbr.wq.merlintohec.model;

import gov.usbr.wq.dataaccess.http.HttpAccessException;
import gov.usbr.wq.dataaccess.http.HttpAccessUtils;
import gov.usbr.wq.dataaccess.jwt.TokenContainer;
import gov.usbr.wq.merlintohec.exceptions.MerlinAccessException;
import hec.io.TimeSeriesContainer;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class MerlinTimeSeriesDaoTest
{
	//@Test
	void testCatalogTimeSeries() throws HttpAccessException, MerlinAccessException
	{
		MerlinTimeSeriesDao dao = new MerlinTimeSeriesDao();
		String username = ResourceAccess.getUsername();
		char[] password = ResourceAccess.getPassword();
		TokenContainer token = HttpAccessUtils.authenticate(username, password);
		Instant start = Instant.parse("2019-01-01T08:00:00.00Z");
		Instant end = Instant.parse("2020-01-01T08:00:00.00Z");
		List<TimeSeriesContainer> containers = dao.catalogTimeSeries(start, end, null, token);
		assertFalse(containers.isEmpty());
	}
}