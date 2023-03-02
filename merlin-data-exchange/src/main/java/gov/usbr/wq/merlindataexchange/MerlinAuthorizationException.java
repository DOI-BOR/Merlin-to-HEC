package gov.usbr.wq.merlindataexchange;

import gov.usbr.wq.dataaccess.http.ApiConnectionInfo;
import gov.usbr.wq.dataaccess.http.HttpAccessException;
import gov.usbr.wq.merlindataexchange.parameters.UsernamePasswordHolder;

final class MerlinAuthorizationException extends Exception
{
    private final HttpAccessException _accessException;
    private final ApiConnectionInfo _connectionInfo;

    MerlinAuthorizationException(HttpAccessException ex, UsernamePasswordHolder usernamePassword, ApiConnectionInfo connectionInfo)
    {
        super("Failed to authenticate user: " + usernamePassword.getUsername(), ex);
        _accessException = ex;
        _connectionInfo = connectionInfo;
    }

    HttpAccessException getAccessException()
    {
        return _accessException;
    }

    ApiConnectionInfo getConnectionInfo()
    {
        return _connectionInfo;
    }
}
