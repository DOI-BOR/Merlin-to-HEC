package gov.usbr.wq.merlindataexchange.io;

public final class MerlinInvalidTimestepException extends Exception
{
    public MerlinInvalidTimestepException(String timestep, String seriesId)
    {
        super("Missing or Unsupported timestep: " + timestep + ". Skipping extract of " + seriesId);
    }

}
