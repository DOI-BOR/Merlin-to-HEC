package gov.usbr.wq.merlindataexchange.io;

import java.util.concurrent.locks.ReentrantLock;

public final class CloseableReentrantLock extends ReentrantLock implements AutoCloseable
{
    @Override
    public void close()
    {
        unlock();
    }

    public CloseableReentrantLock lockIt()
    {
        this.lock();
        return this;
    }
}
