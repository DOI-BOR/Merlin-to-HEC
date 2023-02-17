package gov.usbr.wq.merlindataexchange;

import java.util.concurrent.CompletableFuture;

public interface DataExchangeEngine
{
    CompletableFuture<MerlinDataExchangeStatus> runExtract();

    void cancelExtract();
}
