package gov.usbr.wq.merlindataexchange;

public final class AuthenticationParameters
{

    private final String _url;
    private final UsernamePasswordHolder _usernamePassword;

    AuthenticationParameters(String url, UsernamePasswordHolder usernamePassword)
    {
        _url = url;
        _usernamePassword = usernamePassword;
    }

    String getUrl()
    {
        return _url;
    }

    UsernamePasswordHolder getUsernamePassword()
    {
        return _usernamePassword;
    }
}
