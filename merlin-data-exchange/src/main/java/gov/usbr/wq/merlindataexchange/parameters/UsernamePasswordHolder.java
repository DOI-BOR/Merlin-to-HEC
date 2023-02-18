package gov.usbr.wq.merlindataexchange.parameters;

public final class UsernamePasswordHolder
{
    private final String _username;
    private final char[] _password;

    UsernamePasswordHolder(String username, char[] password)
    {
        _username = username;
        _password = password;
    }

    public String getUsername()
    {
        return _username;
    }

    public char[] getPassword()
    {
        return _password;
    }
}
