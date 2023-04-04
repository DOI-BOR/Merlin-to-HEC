package gov.usbr.wq.merlindataexchange.parameters;

public final class AuthenticationParameters
{

    private final String _url;
    private final UsernamePasswordHolder _usernamePassword;

    AuthenticationParameters(String url, UsernamePasswordHolder usernamePassword)
    {
        _url = url;
        _usernamePassword = usernamePassword;
    }

    public String getUrl()
    {
        return _url;
    }

    public String getUsername()
    {
        return _usernamePassword.getUsername();
    }

    public UsernamePasswordHolder getUsernamePassword()
    {
        return _usernamePassword;
    }
}

