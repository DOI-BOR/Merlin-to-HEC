package gov.usbr.wq.merlindataexchange.integration;

import gov.usbr.wq.merlindataexchange.DataExchangeEngine;
import gov.usbr.wq.merlindataexchange.MerlinDataExchangeStatus;
import gov.usbr.wq.merlindataexchange.MerlinDataExchangeEngineBuilder;
import gov.usbr.wq.merlindataexchange.ResourceAccess;
import gov.usbr.wq.merlindataexchange.TestLogProgressListener;
import gov.usbr.wq.merlindataexchange.parameters.AuthenticationParametersBuilder;
import gov.usbr.wq.merlindataexchange.parameters.MerlinParameters;
import gov.usbr.wq.merlindataexchange.parameters.MerlinParametersBuilder;
import hec.io.impl.StoreOptionImpl;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

final class MerlinDataExchangeIntegrationTest
{
    @Test
    void testRunExtract() throws IOException
    {
        String username = ResourceAccess.getUsername();
        char[] password = ResourceAccess.getPassword();
        String mockXmlFileName = "merlin_mock_config_dx.xml";
        Path mockXml = getMockXml(mockXmlFileName);
        List<Path> mocks = Collections.singletonList(mockXml);
        Path testDirectory = getTestDirectory();
        Instant start = Instant.parse("2016-02-01T12:00:00Z");
        Instant end = Instant.parse("2016-02-21T12:00:00Z");
        StoreOptionImpl storeOption = new StoreOptionImpl();
        storeOption.setRegular("0-replace-all");
        storeOption.setIrregular("0-delete_insert");
        MerlinParameters parameters = new MerlinParametersBuilder()
                .withWatershedDirectory(testDirectory)
                .withLogFileDirectory(testDirectory)
                .withAuthenticationParameters(new AuthenticationParametersBuilder()
                        .forUrl("https://www.grabdata2.com")
                        .setUsername(username)
                        .andPassword(password)
                        .build())
                .withStoreOption(storeOption)
                .withStart(start)
                .withEnd(end)
                .withFPartOverride("fPart")
                .build();
        DataExchangeEngine dataExchangeEngine = new MerlinDataExchangeEngineBuilder()
                .withConfigurationFiles(mocks)
                .withParameters(parameters)
                .withProgressListener(new TestLogProgressListener())
                .build();
        MerlinDataExchangeStatus status = dataExchangeEngine.runExtract().join();
        assertEquals(MerlinDataExchangeStatus.COMPLETE_SUCCESS, status);
    }

    private Path getTestDirectory()
    {
        return Paths.get(System.getProperty("user.dir")).resolve("build/tmp");
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
