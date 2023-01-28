package gov.usbr.wq.merlintohec.model;

import gov.usbr.wq.dataaccess.json.Data;
import gov.usbr.wq.dataaccess.json.Event;
import gov.usbr.wq.dataaccess.model.DataWrapper;
import gov.usbr.wq.merlintohec.exceptions.MerlinDataException;
import hec.heclib.util.HecTime;
import hec.io.TimeSeriesContainer;
import hec.lang.Const;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class MerlinDataConverterTest
{
	private static final String TEST_TIMESERIES_ID = "Shasta Lake-Shasta Dam-Outflow/Flow/INST-VAL/60/0/35-230.11.125.1.1";

//	@Test
	void dataWrapperToTimeSeries() throws MerlinDataException
	{
		ZonedDateTime startTime = ZonedDateTime.now()
											   .withYear(2019)
											   .withDayOfYear(1)
											   .withHour(0)
											   .withMinute(0)
											   .withSecond(0)
											   .withNano(0);
		ZonedDateTime endTime = ZonedDateTime.now()
											 .withYear(2019)
											 .withDayOfYear(30)
											 .withHour(0)
											 .withMinute(0)
											 .withSecond(0)
											 .withNano(0);
		Data data = new Data().seriesString(TEST_TIMESERIES_ID);
		ZonedDateTime curTime = startTime;
		Double value = null;

		List<Double> values = new ArrayList<>();
		List<Integer> times = new ArrayList<>();

		while (curTime.isBefore(endTime))
		{
			HecTime hecTime = MerlinDataConverter.fromZonedDateTime(curTime);
			times.add(hecTime.value());
			values.add(value);
			data.addEventsItem(new Event().date(curTime.toOffsetDateTime())
										  .value(value));
			if (value == null)
			{
				value = 0.0;
			}
			value += 1;
			curTime = curTime.plusHours(1);
		}

		double[] expectedVals = values.stream()
									  .mapToDouble(val -> {
										  double out = Const.UNDEFINED_DOUBLE;
										  if (val != null)
										  {
											  out = val;
										  }
										  return out;
									  })
									  .toArray();
		int[] expectedTimes = times.stream()
								   .mapToInt(Integer::intValue)
								   .toArray();

		DataWrapper wrapper = new DataWrapper(data);
		TimeSeriesContainer tsc = MerlinDataConverter.dataToTimeSeries(wrapper);
		int[] receivedTimes = tsc.times;
		double[] receivedValues = tsc.values;

		assertArrayEquals(expectedTimes, receivedTimes);
		assertArrayEquals(expectedVals, receivedValues, 0.0001);
	}
}