package gov.usbr.wq.merlindataexchange;

import gov.usbr.wq.dataaccess.http.ApiConnectionInfo;
import gov.usbr.wq.dataaccess.http.HttpAccessException;
import gov.usbr.wq.merlindataexchange.parameters.UsernamePasswordHolder;

import java.net.UnknownHostException;

public final class MerlinAuthorizationException extends Exception
{
    public MerlinAuthorizationException(HttpAccessException ex, UsernamePasswordHolder usernamePassword, ApiConnectionInfo connectionInfo)
    {
        super(getMessageFromHttpAccessException("Failed to authenticate user: " + usernamePassword.getUsername(), ex, connectionInfo), ex);
    }

    private static String getMessageFromHttpAccessException(String errorMsg, HttpAccessException e, ApiConnectionInfo connectionInfo)
    {
        String retVal = errorMsg;
        if(e.getResponseMessage() != null)
        {
            retVal += " for URL: " + connectionInfo.getApiRoot() + " | Error code: " + e.getResponseCode() + " (" + e.getResponseMessage() + ")";
        }
        else if(e.getCause() instanceof UnknownHostException)
        {
            retVal += " for unknown URL: " + connectionInfo.getApiRoot();
        }
        return retVal;
    }
}
