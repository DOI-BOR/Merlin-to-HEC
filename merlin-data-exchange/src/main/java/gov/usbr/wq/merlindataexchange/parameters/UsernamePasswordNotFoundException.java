package gov.usbr.wq.merlindataexchange.parameters;

public final class UsernamePasswordNotFoundException extends Exception
{
    public UsernamePasswordNotFoundException(String url)
    {
        super("Failed to find username/password associated with url: " + url + ". Please verify parameters are correct");
    }
}
