package gov.usbr.wq.merlintohec.exceptions;

public final class MerlinInvalidTimestepException extends Exception
{
    public MerlinInvalidTimestepException(String timestep, String seriesId)
    {
        super("Missing or Unsupported timestep: " + timestep + ". Skipping extract of " + seriesId);
    }

}
