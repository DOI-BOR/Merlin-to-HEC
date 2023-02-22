package gov.usbr.wq.merlindataexchange;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

public final class MerlinThreadFactory implements ThreadFactory
{

    private static final AtomicInteger POOL_NUMBER = new AtomicInteger(1);
    private final ThreadGroup _group;
    private final AtomicInteger _threadNumber = new AtomicInteger(1);
    private final String _namePrefix;

    public MerlinThreadFactory()
    {
        _group = Thread.currentThread().getThreadGroup();
        _namePrefix = "merlin-" +
                POOL_NUMBER.getAndIncrement() +
                "-thread-";
    }

    public Thread newThread(Runnable r)
    {
        Thread t = new Thread(_group, r,
                _namePrefix + _threadNumber.getAndIncrement(),
                0);
        if (t.isDaemon())
            t.setDaemon(false);
        if (t.getPriority() != Thread.NORM_PRIORITY)
            t.setPriority(Thread.NORM_PRIORITY);
        return t;
    }
}

