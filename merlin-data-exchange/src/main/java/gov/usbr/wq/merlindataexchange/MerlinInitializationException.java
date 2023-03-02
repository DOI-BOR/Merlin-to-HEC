package gov.usbr.wq.merlindataexchange;

import gov.usbr.wq.dataaccess.http.ApiConnectionInfo;
import gov.usbr.wq.dataaccess.http.HttpAccessException;

import java.net.UnknownHostException;

final class MerlinInitializationException extends Exception
{
    MerlinInitializationException(ApiConnectionInfo connectionInfo, Exception e)
    {
        super(getMessageFromHttpAccessException("Failed to initialize templates, quality versions, and measures", e, connectionInfo), e);
    }

    private static String getMessageFromHttpAccessException(String errorMsg, Exception e, ApiConnectionInfo connectionInfo)
    {
        if (e instanceof HttpAccessException)
        {
            HttpAccessException accessException = (HttpAccessException) e;
            if(accessException.getResponseMessage() != null)
            {
                errorMsg += " for URL: " + connectionInfo.getApiRoot() + " | Error code: " + accessException.getResponseCode() + " (" + accessException.getResponseMessage() + ")";
            }
            else if(accessException.getCause() instanceof UnknownHostException)
            {
                errorMsg += " for unknown URL: " + connectionInfo.getApiRoot();
            }
        }
        else
        {
            //IOException only occurs if the object mapper failed in merlin web client
            errorMsg += " for URL: " + connectionInfo.getApiRoot() + " | Error converting data from Merlin into Java object";
        }
        return errorMsg;
    }
}
