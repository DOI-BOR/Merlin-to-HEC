package gov.usbr.wq.merlindataexchange;

public final class UsernamePasswordNotFoundException extends Exception
{
    UsernamePasswordNotFoundException(String url)
    {
        super("Failed to find username/password associated with url: " + url + ". Please verify parameters are correct");
    }
}
