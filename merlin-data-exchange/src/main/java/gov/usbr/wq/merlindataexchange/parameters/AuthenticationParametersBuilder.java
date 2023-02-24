package gov.usbr.wq.merlindataexchange.parameters;

import gov.usbr.wq.merlindataexchange.fluentbuilders.FluentAuthenticationBuilder;
import gov.usbr.wq.merlindataexchange.fluentbuilders.FluentAuthenticationPassword;
import gov.usbr.wq.merlindataexchange.fluentbuilders.FluentAuthenticationUrl;
import gov.usbr.wq.merlindataexchange.fluentbuilders.FluentAuthenticationUsername;

import java.util.Objects;

public final class AuthenticationParametersBuilder implements FluentAuthenticationUrl
{


    private String _url;
    private String _username;
    private char[] _password;

    @Override
    public FluentAuthenticationUsername forUrl(String url)
    {
        _url = Objects.requireNonNull(url, "URL must be specified, not null");
        return new FluentAuthenticationUsernameImpl();
    }

    private class FluentAuthenticationUsernameImpl implements FluentAuthenticationUsername
    {

        @Override
        public FluentAuthenticationPassword setUsername(String username)
        {
            _username = Objects.requireNonNull(username, "Username must be specified, not null");
            return new FluentAuthenticationPasswordImpl();
        }
    }

    private class FluentAuthenticationPasswordImpl implements FluentAuthenticationPassword
    {

        @Override
        public FluentAuthenticationBuilder andPassword(char[] password)
        {
            _password = Objects.requireNonNull(password, "Password must be specified, not null");
            return new FluentAuthenticationBuildImpl();
        }
    }

    private class FluentAuthenticationBuildImpl implements FluentAuthenticationBuilder
    {

        @Override
        public AuthenticationParameters build()
        {
            return new AuthenticationParameters(_url, new UsernamePasswordHolder(_username, _password));
        }
    }
}
