package gov.usbr.wq.merlindataexchange;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Level;
import java.util.logging.Logger;

public abstract class MerlinEngine
{
    private static Logger LOGGER = Logger.getLogger(MerlinEngine.class.getName());
    private static final int THREAD_COUNT = 5;
    private static final String THREAD_PROPERTY_KEY = "merlin.engine.threadpool.size";
    private final ExecutorService _executorService = Executors.newFixedThreadPool(MerlinEngine.getThreadPoolSize(), new MerlinThreadFactory());

    private static int getThreadPoolSize()
    {
        int retVal;
        //availableProcessors gives us logical cores, which includes hyperthreading stuff.  We can't determine if hyperthreading is on, so let's always halve the available processors.
        //Let's make sure we don't go lower than 1 by using Math.max.  1 / 2 = 0 in integer values, so this could be bad...
        String threadPoolSize = System.getProperty(THREAD_PROPERTY_KEY);
        if (threadPoolSize != null)
        {
            retVal = Math.max(Integer.parseInt(threadPoolSize), 1);
            LOGGER.log(Level.FINE, () -> "Merlin executor service created using System Property " + THREAD_PROPERTY_KEY + " with thread pool size of: " + retVal);
        } else
        {
            int coreCount = Math.max(MerlinEngine.getCoreCount(), 1);
            retVal = THREAD_COUNT * coreCount; //5 should cover bases for concurrent merlin web data retrieval?
            LOGGER.log(Level.FINE, () -> "System Property " + THREAD_PROPERTY_KEY + " not set. Merlin executor service created using default thread pool size of: " + retVal);
        }
        return retVal;
    }

    private static int getCoreCount()
    {
        return Runtime.getRuntime().availableProcessors() / 2;
    }

    protected ExecutorService getExecutorService()
    {
        return _executorService;
    }
}
