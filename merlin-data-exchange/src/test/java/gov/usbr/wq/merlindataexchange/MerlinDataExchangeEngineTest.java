package gov.usbr.wq.merlindataexchange;

import gov.usbr.wq.dataaccess.http.HttpAccessException;
import gov.usbr.wq.merlindataexchange.parameters.AuthenticationParametersBuilder;
import gov.usbr.wq.merlindataexchange.parameters.MerlinParameters;
import gov.usbr.wq.merlindataexchange.parameters.MerlinParametersBuilder;
import hec.io.impl.StoreOptionImpl;
import hec.ui.ProgressListener;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.logging.Logger;

import static org.junit.jupiter.api.Assertions.assertEquals;

final class MerlinDataExchangeEngineTest
{

    private static final Logger LOGGER = Logger.getLogger(MerlinDataExchangeEngineTest.class.getName());
    @Test
    void testRunExtract() throws IOException
    {
        String username = ResourceAccess.getUsername();
        char[] password = ResourceAccess.getPassword();
        Path mockXml = getMockXml("merlin_mock_config_dx.xml");
        Path mockXml2 = getMockXml("merlin_mock_config_dx2.xml");
        List<Path> mocks = Arrays.asList(mockXml, mockXml2);
        Path workingDir = Paths.get(System.getProperty("user.dir"));
        Instant start = Instant.parse("2019-01-01T08:00:00Z");
        Instant end = Instant.parse("2022-08-30T08:00:00Z");
        StoreOptionImpl storeOption = new StoreOptionImpl();
        storeOption.setRegular("0-replace-all");
        storeOption.setIrregular("0-delete_insert");
        MerlinParameters params = new MerlinParametersBuilder()
                .withWatershedDirectory(workingDir)
                .withLogFileDirectory(workingDir)
                .withStart(start)
                .withEnd(end)
                .withStoreOption(storeOption)
                .withFPartOverride("fPart")
                .withAuthenticationParameters(new AuthenticationParametersBuilder()
                        .forUrl("https://www.grabdata2.com")
                        .setUsername(username)
                        .andPassword(password)
                        .build())
                .build();
        DataExchangeEngine dataExchangeEngine = new MerlinExchangeEngineBuilder()
                .withConfigurationFiles(mocks)
                .withParameters(params)
                .withProgressListener(buildLoggingProgressListener())
                .build();
        MerlinDataExchangeStatus status = dataExchangeEngine.runExtract().join();
        assertEquals(MerlinDataExchangeStatus.COMPLETE_SUCCESS, status);
    }

    void testRunExtractCancelled() throws IOException, HttpAccessException, InterruptedException {
        String username = ResourceAccess.getUsername();
        char[] password = ResourceAccess.getPassword();
        Path mockXml = getMockXml("merlin_mock_config_dx.xml");
        Path workingDir = Paths.get(System.getProperty("user.dir"));
        Instant start = Instant.parse("2019-01-01T08:00:00Z");
        Instant end = Instant.parse("2022-08-30T08:00:00Z");
        StoreOptionImpl storeOption = new StoreOptionImpl();
        storeOption.setRegular("0-replace-all");
        storeOption.setIrregular("0-delete_insert");
        MerlinParameters params = new MerlinParametersBuilder()
                .withWatershedDirectory(workingDir)
                .withLogFileDirectory(workingDir)
                .withStart(start)
                .withEnd(end)
                .withStoreOption(storeOption)
                .withFPartOverride("fPart")
                .withAuthenticationParameters(new AuthenticationParametersBuilder()
                        .forUrl("https://www.grabdata2.com")
                        .setUsername(username)
                        .andPassword(password)
                        .build())
                .build();
        DataExchangeEngine dataExchangeEngine = new MerlinExchangeEngineBuilder()
                .withConfigurationFiles(Collections.singletonList(mockXml))
                .withParameters(params)
                .withProgressListener(buildLoggingProgressListener())
                .build();
        dataExchangeEngine.runExtract();
        Thread.sleep(500);
        dataExchangeEngine.cancelExtract();
        Thread.sleep(20000);
    }

    private ProgressListener buildLoggingProgressListener()
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
                progress(i);
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
