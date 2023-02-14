package gov.usbr.wq.merlindataexchange;

import gov.usbr.wq.dataaccess.http.HttpAccessException;
import hec.ui.ProgressListener;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public interface DataExchangeEngine
{
    String LOOKUP_PATH = "dataexchange/engine";

    CompletableFuture<Void> runExtract(List<Path> xmlConfigurationFiles, MerlinDataExchangeParameters runtimeParameters, ProgressListener progressListener) throws IOException, HttpAccessException;

    void cancelExtract();
}
