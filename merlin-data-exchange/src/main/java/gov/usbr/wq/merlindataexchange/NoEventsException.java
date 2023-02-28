package gov.usbr.wq.merlindataexchange;

public final class NoEventsException extends Exception
{
    public NoEventsException(String dssPath, String seriesId)
    {
        super("No data to write to " + dssPath + " from " + seriesId);
    }
}
