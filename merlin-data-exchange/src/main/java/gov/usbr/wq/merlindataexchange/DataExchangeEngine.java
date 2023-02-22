package gov.usbr.wq.merlindataexchange;

import java.util.concurrent.CompletableFuture;

public interface DataExchangeEngine
{
    /**
     *
     * @return CompletableFuture containing status of extract.
     * Calling runExtract() will block any threads if an extract is already running for this DataExchangeEngine instance
     * i.e. If you want to run extracts in parallel, multiple DataExchangeEngines must be built
     */
    CompletableFuture<MerlinDataExchangeStatus> runExtract();

    void cancelExtract();
}
