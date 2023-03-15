package gov.usbr.wq.merlindataexchange.io;

final class ReadWriteLockManager
{

    private final CloseableReentrantLock _closeableReentrantLock = new CloseableReentrantLock();

    private ReadWriteLockManager()
    {

    }
    public static ReadWriteLockManager getInstance()
    {
        return SingletonHelper.INSTANCE;
    }

    public CloseableReentrantLock getCloseableLock()
    {
        return _closeableReentrantLock;
    }

    private static class SingletonHelper
    {
        private static final ReadWriteLockManager INSTANCE = new ReadWriteLockManager();
    }

}
