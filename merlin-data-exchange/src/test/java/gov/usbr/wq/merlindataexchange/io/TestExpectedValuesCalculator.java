package gov.usbr.wq.merlindataexchange.io;

import hec.data.DataSetIllegalArgumentException;
import hec.heclib.util.HecTime;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.time.ZoneId;
import java.util.TimeZone;

import static org.junit.jupiter.api.Assertions.assertEquals;

final class TestExpectedValuesCalculator
{

    @Test
    void testCalculateBothOnInterval() throws DataSetIllegalArgumentException
    {
        Instant start = Instant.parse("2022-01-01T08:00:00Z");
        Instant end = Instant.parse("2022-01-04T08:00:00Z");
        HecTime mockFirstTime = new HecTime("2022-01-01T08:00:00Z");
        HecTime mockLastTime = new HecTime("2022-01-04T08:00:00Z");
        ZoneId timeZone = ZoneId.of("UTC");
        int expectedNumValues = DssDataExchangeWriter.getExpectedNumValues(start, end, "1Day", timeZone, mockFirstTime, mockLastTime);
        assertEquals(4, expectedNumValues);

        start = Instant.parse("2022-01-01T08:00:00Z");
        end = Instant.parse("2022-01-05T08:00:00Z");
        mockFirstTime = new HecTime("2022-01-01T08:00:00Z");
        mockLastTime = new HecTime("2022-01-05T08:00:00Z");
        expectedNumValues = DssDataExchangeWriter.getExpectedNumValues(start, end, "1Day", timeZone, mockFirstTime, mockLastTime);
        assertEquals(5, expectedNumValues);

        start = Instant.parse("2022-01-01T08:00:00Z");
        end = Instant.parse("2022-01-02T08:00:00Z");
        mockFirstTime = new HecTime("2022-01-01T08:00:00Z");
        mockLastTime = new HecTime("2022-01-02T08:00:00Z");
        expectedNumValues = DssDataExchangeWriter.getExpectedNumValues(start, end, "1Hour", timeZone, mockFirstTime, mockLastTime);
        assertEquals(25, expectedNumValues);

    }

    @Test
    void testStartIsBeforeAndEndIsOn() throws DataSetIllegalArgumentException
    {
        Instant start = Instant.parse("2022-01-01T08:20:00Z");
        Instant end = Instant.parse("2022-01-04T08:00:00Z");
        HecTime mockFirstTime = new HecTime("2022-01-02T08:00:00Z");
        HecTime mockLastTime = new HecTime("2022-01-04T08:00:00Z");
        ZoneId timeZone = ZoneId.of("UTC");
        int expectedNumValues = DssDataExchangeWriter.getExpectedNumValues(start, end, "1Day", timeZone, mockFirstTime, mockLastTime);
        assertEquals(3, expectedNumValues);

        start = Instant.parse("2022-01-01T08:30:00Z");
        end = Instant.parse("2022-01-02T08:00:00Z");
        mockFirstTime = new HecTime("2022-01-01T09:00:00Z");
        mockLastTime = new HecTime("2022-01-02T08:00:00Z");
        expectedNumValues = DssDataExchangeWriter.getExpectedNumValues(start, end, "1Hour", timeZone, mockFirstTime, mockLastTime);
        assertEquals(24, expectedNumValues);
    }

    @Test
    void testStartIsOnAndEndIsAfter() throws DataSetIllegalArgumentException
    {
        Instant start = Instant.parse("2022-01-01T08:00:00Z");
        Instant end = Instant.parse("2022-01-04T08:30:00Z");
        HecTime mockFirstTime = new HecTime("2022-01-01T08:00:00Z");
        HecTime mockLastTime = new HecTime("2022-01-04T08:00:00Z");
        ZoneId timeZone = ZoneId.of("UTC");
        int expectedNumValues = DssDataExchangeWriter.getExpectedNumValues(start, end, "1Day", timeZone, mockFirstTime, mockLastTime);
        assertEquals(4, expectedNumValues);

        start = Instant.parse("2022-01-01T08:00:00Z");
        end = Instant.parse("2022-01-05T23:59:00Z");
        mockFirstTime = new HecTime("2022-01-01T08:00:00Z");
        mockLastTime = new HecTime("2022-01-04T08:00:00Z");
        expectedNumValues = DssDataExchangeWriter.getExpectedNumValues(start, end, "1Day", timeZone, mockFirstTime, mockLastTime);
        assertEquals(5, expectedNumValues);
    }

    @Test
    void testStartIsBeforeAndEndIsAfter() throws DataSetIllegalArgumentException
    {
        Instant start = Instant.parse("2022-01-01T08:20:00Z");
        Instant end = Instant.parse("2022-01-04T08:20:00Z");
        HecTime mockFirstTime = new HecTime("2022-01-02T08:00:00Z");
        HecTime mockLastTime = new HecTime("2022-01-04T08:00:00Z");
        ZoneId timeZone = ZoneId.of("UTC");
        int expectedNumValues = DssDataExchangeWriter.getExpectedNumValues(start, end, "1Day", timeZone, mockFirstTime, mockLastTime);
        assertEquals(3, expectedNumValues);

        start = Instant.parse("2022-01-01T08:20:00Z");
        end = Instant.parse("2022-01-04T08:59:00Z");
        mockFirstTime = new HecTime("2022-01-02T08:00:00Z");
        mockLastTime = new HecTime("2022-01-04T08:00:00Z");
        expectedNumValues = DssDataExchangeWriter.getExpectedNumValues(start, end, "1Day", timeZone, mockFirstTime, mockLastTime);
        assertEquals(3, expectedNumValues);

        start = Instant.parse("2021-12-31T00:00:01Z");
        end = Instant.parse("2022-01-04T12:59:00Z");
        mockFirstTime = new HecTime("2022-01-01T00:00:00Z");
        mockLastTime = new HecTime("2022-01-04T00:00:00Z");
        expectedNumValues = DssDataExchangeWriter.getExpectedNumValues(start, end, "1Day", timeZone, mockFirstTime, mockLastTime);
        assertEquals(4, expectedNumValues);
    }


}
