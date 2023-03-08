package gov.usbr.wq.merlindataexchange.io;

import hec.heclib.dss.HecTimeSeriesBase;
import hec.heclib.util.HecTime;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;

final class ExpectedNumberValuesCalculator
{

    private ExpectedNumberValuesCalculator()
    {
        throw new AssertionError("Utility class");
    }
    static int getExpectedNumValues(Instant start, Instant end, String ePart, ZoneId tscZoneId, HecTime firstRealTime, HecTime lastRealTime)
    {
        if(start == null)
        {
            start = firstRealTime.getInstant(tscZoneId);
        }
        if(end == null)
        {
            end = lastRealTime.getInstant(tscZoneId);
        }
        int intervalMinutes = HecTimeSeriesBase.getIntervalFromEPart(ePart);
        long durationMinutes = Duration.between(start, end).toMinutes();
        boolean startIsBeforeFirstRealTime = start.isBefore(firstRealTime.getInstant(tscZoneId));
        boolean endIsAfterLastRealTime = end.isAfter(lastRealTime.getInstant(tscZoneId));
        int retVal = (int) (durationMinutes / ((double) intervalMinutes));
        if(!(startIsBeforeFirstRealTime && endIsAfterLastRealTime))
        {
            retVal ++;
        }
        return retVal;
    }
}
