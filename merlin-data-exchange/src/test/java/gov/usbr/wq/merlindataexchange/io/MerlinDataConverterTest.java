package gov.usbr.wq.merlindataexchange.io;

import gov.usbr.wq.dataaccess.json.Data;
import gov.usbr.wq.dataaccess.json.Event;
import gov.usbr.wq.dataaccess.model.DataWrapper;
import gov.usbr.wq.merlindataexchange.NoEventsException;
import hec.data.DataSetIllegalArgumentException;
import hec.data.Units;
import hec.data.UnitsConversionException;
import hec.heclib.util.HecTime;
import hec.hecmath.HecMathException;
import hec.io.TimeSeriesContainer;
import hec.lang.Const;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class MerlinDataConverterTest
{
	private static final String TEST_TIMESERIES_ID = "Shasta Lake-Shasta Dam-Outflow/Flow/INST-VAL/60/0/35-230.11.125.1.1";

	@Test
	void dataWrapperToTimeSeries() throws NoEventsException, DataSetIllegalArgumentException, HecMathException
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
		data.setTimeZone("UTC-08:00");
		ZonedDateTime curTime = startTime;
		Double value = null;

		List<Double> values = new ArrayList<>();
		List<Integer> times = new ArrayList<>();

		while (curTime.isBefore(endTime))
		{
			HecTime hecTime = MerlinDataConverter.fromZonedDateTime(curTime, ZoneId.of(data.getTimeZone()));
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
											  try
											  {
												  out = Units.convertUnits(val, "cfs", "cms");
											  }
											  catch (UnitsConversionException e)
											  {
												  throw new RuntimeException(e);
											  }
										  }
										  return out;
									  })
									  .toArray();
		int[] expectedTimes = times.stream()
								   .mapToInt(Integer::intValue)
								   .toArray();

		data.setUnits("cfs");
		data.setParameter("FLOW");
		DataWrapper wrapper = new DataWrapper(data);
		try
		{
			TimeSeriesContainer tsc = MerlinDataConverter.dataToTimeSeries(wrapper, "SI", null, false, null, "230.6.125.1.1");
			int[] receivedTimes = tsc.times;
			double[] receivedValues = tsc.values;
			assertArrayEquals(expectedTimes, receivedTimes);
			assertArrayEquals(expectedVals, receivedValues, 0.0001);
		}
		catch (MerlinInvalidTimestepException e)
		{

		}

	}

	@Test
	void testTimeZoneConversion()
	{
		ZonedDateTime zuluNow = ZonedDateTime.now(ZoneId.of("Z"));
		HecTime hecTimePST = MerlinDataConverter.fromZonedDateTime(zuluNow, ZoneId.of("UTC-08:00"));
		assertNotEquals(zuluNow.toLocalDateTime(), hecTimePST.getLocalDateTime());
		LocalDateTime expected = zuluNow.toLocalDateTime().minusHours(8).truncatedTo(ChronoUnit.MINUTES);
		assertEquals(expected, hecTimePST.getLocalDateTime());
	}

	@Test
	void testValidTimeSteps() throws MerlinInvalidTimestepException
	{
		String seriesId = "testSeries";
		int dssTimeStep = MerlinDataConverter.getValidTimeStep("43200, 44640, 40320", seriesId);
		assertEquals(MerlinDataConverter.DSS_MONTHLY_TIME_STEP, dssTimeStep);
		dssTimeStep = MerlinDataConverter.getValidTimeStep("43200, 40320", seriesId);
		assertEquals(MerlinDataConverter.DSS_MONTHLY_TIME_STEP, dssTimeStep);
		dssTimeStep = MerlinDataConverter.getValidTimeStep("44640, 40320", seriesId);
		assertEquals(MerlinDataConverter.DSS_MONTHLY_TIME_STEP, dssTimeStep);
		dssTimeStep = MerlinDataConverter.getValidTimeStep("43200, 44640", seriesId);
		assertEquals(MerlinDataConverter.DSS_MONTHLY_TIME_STEP, dssTimeStep);
		dssTimeStep = MerlinDataConverter.getValidTimeStep("43200", seriesId);
		assertEquals(MerlinDataConverter.DSS_MONTHLY_TIME_STEP, dssTimeStep);
		dssTimeStep = MerlinDataConverter.getValidTimeStep("44640", seriesId);
		assertEquals(MerlinDataConverter.DSS_MONTHLY_TIME_STEP, dssTimeStep);
		dssTimeStep = MerlinDataConverter.getValidTimeStep("40320", seriesId);
		assertEquals(MerlinDataConverter.DSS_MONTHLY_TIME_STEP, dssTimeStep);
		assertEquals(15, MerlinDataConverter.getValidTimeStep("15", seriesId));
		assertThrows(MerlinInvalidTimestepException.class, () -> MerlinDataConverter.getValidTimeStep("43200, 44640, 10000", seriesId));
	}

}
