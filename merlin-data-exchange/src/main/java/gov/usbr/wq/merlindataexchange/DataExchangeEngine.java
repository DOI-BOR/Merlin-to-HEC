package gov.usbr.wq.merlindataexchange;

import hec.ui.ProgressListener;

import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public interface DataExchangeEngine
{
    CompletableFuture<MerlinDataExchangeStatus> runExtract(List<Path> xmlConfigurationFiles, MerlinDataExchangeParameters runtimeParameters, ProgressListener progressListener);

    void cancelExtract();
}
