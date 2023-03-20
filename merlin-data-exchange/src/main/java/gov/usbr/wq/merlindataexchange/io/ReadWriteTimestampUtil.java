package gov.usbr.wq.merlindataexchange.io;

import gov.usbr.wq.merlindataexchange.MerlinDataExchangeEngine;

import java.time.Duration;
import java.time.Instant;

final class ReadWriteTimestampUtil
{

    private ReadWriteTimestampUtil()
    {
        throw new AssertionError("Utility class");
    }

    public static String getDuration(Instant startTime, Instant endTime)
    {
        String retVal = "";
        String readTimeStampProperty = System.getProperty(MerlinDataExchangeEngine.READ_WRITE_TIMESTAMP_PROPERTY);
        boolean logReadTimeStamp = false;
        if(readTimeStampProperty != null)
        {
            logReadTimeStamp = Boolean.parseBoolean(readTimeStampProperty);
        }
        if(logReadTimeStamp)
        {
            retVal = " | Duration: " + Duration.between(startTime, endTime).toMillis() + " ms";
        }
        return retVal;
    }
}
