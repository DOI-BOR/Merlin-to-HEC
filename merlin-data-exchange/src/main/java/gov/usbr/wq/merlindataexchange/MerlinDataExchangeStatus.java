package gov.usbr.wq.merlindataexchange;

public enum MerlinDataExchangeStatus
{
    COMPLETE_SUCCESS("All data successfully exchanged"),
    PARTIAL_SUCCESS("Only some data successfully exchanged"),
    FAILURE("No data successfully exchanged"),
    AUTHENTICATION_FAILURE("Failed to authenticate");

    private final String _errorMessage;

    MerlinDataExchangeStatus(String errorMessage)
    {
        _errorMessage = errorMessage;
    }

    public String getErrorMessage()
    {
        return _errorMessage;
    }
}
