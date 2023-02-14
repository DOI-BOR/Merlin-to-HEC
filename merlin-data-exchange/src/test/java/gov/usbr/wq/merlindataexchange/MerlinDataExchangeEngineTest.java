package gov.usbr.wq.merlindataexchange;

import gov.usbr.wq.dataaccess.http.HttpAccessException;
import hec.io.impl.StoreOptionImpl;
import hec.ui.ProgressListener;
import org.junit.jupiter.api.Test;
import rma.util.lookup.Lookup;
import rma.util.lookup.Lookups;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.Collections;
import java.util.logging.Logger;

import static org.junit.jupiter.api.Assertions.assertNotNull;

final class MerlinDataExchangeEngineTest
{

    private static final Logger LOGGER = Logger.getLogger(MerlinDataExchangeEngineTest.class.getName());
    @Test
    void testRunExtract() throws IOException, HttpAccessException, InterruptedException {
        String username = ResourceAccess.getUsername();
        char[] password = ResourceAccess.getPassword();
        Path mockXml = getMockXml("merlin_mock_config_dx.xml");
        Path workingDir = Paths.get(System.getProperty("user.dir"));
        Instant start = Instant.parse("2019-01-01T08:00:00Z");
        Instant end = Instant.parse("2022-08-30T08:00:00Z");
        StoreOptionImpl storeOption = new StoreOptionImpl();
        MerlinDataExchangeParameters params = new MerlinDataExchangeParameters(username, password, workingDir, start, end, storeOption, "fPart");
        String lookupPath = DataExchangeEngine.LOOKUP_PATH + "/" + MerlinDataExchangeEngine.MERLIN;
        Lookup lookup = Lookups.forPath(lookupPath);
        DataExchangeEngine dataExchangeEngine = lookup.lookup(DataExchangeEngine.class);
        assertNotNull(dataExchangeEngine);
        ProgressListener progressListener = buildMockProgressListener();
        dataExchangeEngine.runExtract(Collections.singletonList(mockXml), params, progressListener).join();
    }

    @Test
    void testRunExtractCancelled() throws IOException, HttpAccessException, InterruptedException {
        String username = ResourceAccess.getUsername();
        char[] password = ResourceAccess.getPassword();
        Path mockXml = getMockXml("merlin_mock_config_dx.xml");
        Path workingDir = Paths.get(System.getProperty("user.dir"));
        Instant start = Instant.parse("2019-01-01T08:00:00Z");
        Instant end = Instant.parse("2022-08-30T08:00:00Z");
        StoreOptionImpl storeOption = new StoreOptionImpl();
        MerlinDataExchangeParameters params = new MerlinDataExchangeParameters(username, password, workingDir, start, end, storeOption, "fPart");
        String lookupPath = DataExchangeEngine.LOOKUP_PATH + "/" + MerlinDataExchangeEngine.MERLIN;
        Lookup lookup = Lookups.forPath(lookupPath);
        DataExchangeEngine dataExchangeEngine = lookup.lookup(DataExchangeEngine.class);
        assertNotNull(dataExchangeEngine);
        ProgressListener progressListener = buildMockProgressListener();
        dataExchangeEngine.runExtract(Collections.singletonList(mockXml), params, progressListener);
        Thread.sleep(500);
        dataExchangeEngine.cancelExtract();
        Thread.sleep(20000);
    }

    private ProgressListener buildMockProgressListener()
    {
        return new ProgressListener()
        {
            @Override
            public void start()
            {
                LOGGER.info(() -> "started");
            }

            @Override
            public void start(int i)
            {

            }

            @Override
            public void switchToIndeterminate()
            {

            }

            @Override
            public void setStayOnTop(boolean b)
            {

            }

            @Override
            public void switchToDeterminate(int i)
            {

            }

            @Override
            public void finish()
            {
                LOGGER.info(() -> "Finished!");
            }

            @Override
            public void progress(int i)
            {
                LOGGER.info(() -> "Progress: " + i + "%");
            }

            @Override
            public void progress(String s)
            {
                LOGGER.info(() -> s);
            }

            @Override
            public void progress(String s, MessageType messageType)
            {
                if(messageType == MessageType.IMPORTANT)
                {
                    LOGGER.info(() -> s);
                }
                if(messageType == MessageType.ERROR)
                {
                    LOGGER.warning(() -> s);
                }
            }

            @Override
            public void progress(String s, int i)
            {

            }

            @Override
            public void progress(String s, MessageType messageType, int i)
            {
                if(messageType == MessageType.IMPORTANT)
                {
                    LOGGER.info(() -> s);
                }
                if(messageType == MessageType.ERROR)
                {
                    LOGGER.warning(() -> s);
                }
                LOGGER.info(() -> "Progress: " + i + "%");
            }

            @Override
            public void incrementProgress(int i)
            {

            }
        };
    }

    private Path getMockXml(String fileName) throws IOException
    {
        String resource = "gov/usbr/wq/merlindataexchange/" + fileName;
        URL resourceUrl = getClass().getClassLoader().getResource(resource);
        if (resourceUrl == null)
        {
            throw new IOException("Failed to get resource: " + resource);
        }
        return new File(resourceUrl.getFile()).toPath();
    }
}
