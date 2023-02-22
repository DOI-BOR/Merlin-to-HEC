package gov.usbr.wq.merlindataexchange;

public final class MerlinExchangeDaoCompletionTracker
{
    private static final int TASKS_TO_PERFORM_PER_MEASURE = 2; //1 for read and 1 for write
    private int _numberOfMeasuresToComplete;
    private final int _percentCompleteBeforeReadAndWrite;
    private int _totalCompleted = 0;
    private int _writesCompleted = 0;
    private int _readsCompleted = 0;

    public MerlinExchangeDaoCompletionTracker(int percentCompleteBeforeReadAndWrite)
    {
        _percentCompleteBeforeReadAndWrite = percentCompleteBeforeReadAndWrite;
    }

    public void addNumberOfMeasuresToComplete(int numberToAdd)
    {
        _numberOfMeasuresToComplete += numberToAdd;
    }

    private int readWriteTaskCompleted()
    {
        _totalCompleted++;
        int totalNumOfTasksToBeCompleted = _numberOfMeasuresToComplete * TASKS_TO_PERFORM_PER_MEASURE;
        int weightForReadWriteTasks = 100 - _percentCompleteBeforeReadAndWrite;
        int weightedCompletedPercentage = (int) (weightForReadWriteTasks * ((double) _totalCompleted /totalNumOfTasksToBeCompleted)); //convert to percentage int
        return weightedCompletedPercentage + _percentCompleteBeforeReadAndWrite;
    }

    public int writeTaskCompleted()
    {
        _writesCompleted++;
        return readWriteTaskCompleted();
    }

    public int readTaskCompleted()
    {
        _readsCompleted++;
        return readWriteTaskCompleted();
    }

    public MerlinDataExchangeStatus getCompletionStatus()
    {
        MerlinDataExchangeStatus retVal = MerlinDataExchangeStatus.FAILURE;
        int totalNumOfTasksToBeCompleted= _numberOfMeasuresToComplete * TASKS_TO_PERFORM_PER_MEASURE;
        if(_totalCompleted == totalNumOfTasksToBeCompleted)
        {
            retVal = MerlinDataExchangeStatus.COMPLETE_SUCCESS;
        }
        else if(_writesCompleted > 0 && _readsCompleted > 0)
        {
            retVal = MerlinDataExchangeStatus.PARTIAL_SUCCESS;
        }
        return retVal;
    }

}
