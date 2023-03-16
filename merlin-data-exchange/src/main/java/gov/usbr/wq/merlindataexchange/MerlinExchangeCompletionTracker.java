package gov.usbr.wq.merlindataexchange;

import gov.usbr.wq.merlindataexchange.io.CloseableReentrantLock;

import java.util.concurrent.atomic.AtomicInteger;

public final class MerlinExchangeCompletionTracker
{
    private static final int TASKS_TO_PERFORM_PER_MEASURE = 2; //1 for read and 1 for write
    private int _numberOfMeasuresToComplete;
    private final int _percentCompleteBeforeReadAndWrite;
    private final AtomicInteger _totalCompleted = new AtomicInteger(0);
    private int _writesCompleted = 0;
    private final CloseableReentrantLock _lock = new CloseableReentrantLock();

    public MerlinExchangeCompletionTracker(int percentCompleteBeforeReadAndWrite)
    {
        _percentCompleteBeforeReadAndWrite = percentCompleteBeforeReadAndWrite;
    }

    public void addNumberOfMeasuresToComplete(int numberToAdd)
    {
        _numberOfMeasuresToComplete += numberToAdd;
    }

    public int readWriteTaskCompleted()
    {
        try(CloseableReentrantLock lock = _lock.lockIt())
        {
            int total = _totalCompleted.incrementAndGet();
            int totalNumOfTasksToBeCompleted = _numberOfMeasuresToComplete * TASKS_TO_PERFORM_PER_MEASURE;
            int weightForReadWriteTasks = 100 - _percentCompleteBeforeReadAndWrite;
            int weightedCompletedPercentage = (int) (weightForReadWriteTasks * ((double) total /totalNumOfTasksToBeCompleted)); //convert to percentage int
            return weightedCompletedPercentage + _percentCompleteBeforeReadAndWrite;
        }
    }

    public void writeTaskCompleted()
    {
        _writesCompleted ++;
    }

    public MerlinDataExchangeStatus getCompletionStatus()
    {
        MerlinDataExchangeStatus retVal = MerlinDataExchangeStatus.FAILURE;
        int totalNumOfTasksToBeCompleted= _numberOfMeasuresToComplete * TASKS_TO_PERFORM_PER_MEASURE;
        if(_totalCompleted.get() == totalNumOfTasksToBeCompleted)
        {
            retVal = MerlinDataExchangeStatus.COMPLETE_SUCCESS;
        }
        else if(_writesCompleted > 0)
        {
            retVal = MerlinDataExchangeStatus.PARTIAL_SUCCESS;
        }
        return retVal;
    }

    public void reset()
    {
        _numberOfMeasuresToComplete = 0;
        _writesCompleted = 0;
        _totalCompleted.set(0);
    }

}
