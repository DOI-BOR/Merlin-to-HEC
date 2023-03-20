package gov.usbr.wq.merlindataexchange;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.TimeUnit;

final class MerlinDurationFormatter
{

    private MerlinDurationFormatter()
    {
        throw new AssertionError("Utility class");
    }
    public static String getFormattedDuration(Instant startTime, Instant endTime)
    {
        long millis = Duration.between(startTime, endTime).toMillis();
        long days = TimeUnit.MILLISECONDS.toDays(millis);
        millis -= TimeUnit.DAYS.toMillis(days);
        long hours = TimeUnit.MILLISECONDS.toHours(millis);
        millis -= TimeUnit.HOURS.toMillis(hours);
        long minutes = TimeUnit.MILLISECONDS.toMinutes(millis);
        millis -= TimeUnit.MINUTES.toMillis(minutes);
        double seconds = millis/1000.0;

        StringBuilder sb = new StringBuilder(64);
        if(days == 1)
        {
            sb.append(1 + " Day ");
        }
        else if(days > 1)
        {
            sb.append(days);
            sb.append(" Days ");
        }
        if(hours == 1)
        {
            sb.append(1 + " Hour ");
        }
        else if (hours > 1)
        {
            sb.append(hours);
            sb.append(" Hours ");
        }
        if(minutes == 1)
        {
            sb.append(1 + " Minute ");
        }
        else if (minutes > 1)
        {
            sb.append(minutes);
            sb.append(" Minutes ");
        }
        if(seconds == 1)
        {
            sb.append(1 + " Second");
        }
        else if (seconds >= 0)
        {
            sb.append(String.format("%.2f", seconds));
            sb.append(" Seconds");
        }
        return(sb.toString());
    }

}
