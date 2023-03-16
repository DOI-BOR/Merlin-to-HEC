package gov.usbr.wq.merlindataexchange.io;

import gov.usbr.wq.dataaccess.http.ApiConnectionInfo;
import gov.usbr.wq.dataaccess.http.HttpAccessException;
import gov.usbr.wq.dataaccess.http.HttpAccessUtils;
import gov.usbr.wq.dataaccess.http.TokenContainer;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

final class TokenRegistry
{
    private final Map<ApiConnectionInfo, TokenContainer> _urlTokenMap = new ConcurrentHashMap<>();
    private final Map<ApiConnectionInfo, ReentrantLock> _urlLockMap = new ConcurrentHashMap<>();

    private TokenRegistry()
    {
    }

    static TokenRegistry getRegistry()
    {
        return SingletonHelper.INSTANCE;
    }

    TokenContainer getToken(ApiConnectionInfo connectionInfo, String username, char[] password) throws HttpAccessException
    {
        // Try to get the token without acquiring any locks
        TokenContainer token = _urlTokenMap.get(connectionInfo);

        if (token == null || token.isExpired())
        {
            //one lock per url
            ReentrantLock lock = _urlLockMap.computeIfAbsent(connectionInfo, k -> new ReentrantLock());
            try
            {
                //block for this url if we need to generate a new token and store in registry
                lock.lock();
                token = _urlTokenMap.get(connectionInfo);
                //check again in case another thread may have registered a token while waiting for lock
                if (token == null || token.isExpired())
                {
                    token = HttpAccessUtils.authenticate(connectionInfo, username, password);
                    _urlTokenMap.put(connectionInfo, token);
                }
            }
            finally
            {
                lock.unlock();
            }
        }
        return token;
    }

    private static class SingletonHelper
    {
        private static final TokenRegistry INSTANCE = new TokenRegistry();
    }
}
