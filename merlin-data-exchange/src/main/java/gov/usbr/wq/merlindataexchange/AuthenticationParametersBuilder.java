package gov.usbr.wq.merlindataexchange;

import gov.usbr.wq.merlindataexchange.fluentbuilders.FluentAuthenticationBuild;
import gov.usbr.wq.merlindataexchange.fluentbuilders.FluentAuthenticationPassword;
import gov.usbr.wq.merlindataexchange.fluentbuilders.FluentAuthenticationUrl;
import gov.usbr.wq.merlindataexchange.fluentbuilders.FluentAuthenticationUsername;

public final class AuthenticationParametersBuilder implements FluentAuthenticationUrl
{


    private String _url;
    private String _username;
    private char[] _password;

    @Override
    public FluentAuthenticationUsername forUrl(String url)
    {
        _url = url;
        return new FluentAuthenticationUsernameImpl();
    }

    private class FluentAuthenticationUsernameImpl implements FluentAuthenticationUsername
    {

        @Override
        public FluentAuthenticationPassword setUsername(String username)
        {
            _username = username;
            return new FluentAuthenticationPasswordImpl();
        }
    }

    private class FluentAuthenticationPasswordImpl implements FluentAuthenticationPassword
    {

        @Override
        public FluentAuthenticationBuild andPassword(char[] password)
        {
            _password = password;
            return new FluentAuthenticationBuildImpl();
        }
    }

    private class FluentAuthenticationBuildImpl implements FluentAuthenticationBuild
    {

        @Override
        public AuthenticationParameters build()
        {
            return new AuthenticationParameters(_url, new UsernamePasswordHolder(_username, _password));
        }
    }
}
