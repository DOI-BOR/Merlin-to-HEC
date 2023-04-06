package gov.usbr.wq.merlindataexchange;

import java.util.concurrent.CompletableFuture;

public interface DataExportEngine
{
    CompletableFuture<MerlinDataExchangeStatus> runExport();
}
