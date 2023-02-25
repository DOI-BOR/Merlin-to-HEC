package gov.usbr.wq.merlindataexchange;

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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

final class MerlinDataExchangeEngineTest
{
    @Test
    void testRunExtract() throws IOException
    {
        String username = ResourceAccess.getUsername();
        char[] password = ResourceAccess.getPassword();
        Path mockXml = getMockXml("merlin_mock_config_dx.xml");
        Path mockXml2 = getMockXml("merlin_mock_config_dx2.xml");
        List<Path> mocks = Arrays.asList(mockXml, mockXml2);
        Path testDirectory = getTestDirectory();
        Instant start = Instant.parse("2019-01-01T08:00:00Z");
        Instant end = Instant.parse("2022-08-30T08:00:00Z");
        StoreOptionImpl storeOption = new StoreOptionImpl();
        storeOption.setRegular("0-replace-all");
        storeOption.setIrregular("0-delete_insert");
        MerlinParameters params = new MerlinParametersBuilder()
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
                .withParameters(params)
                .withProgressListener(buildLoggingProgressListener())
                .build();
        MerlinDataExchangeStatus status = dataExchangeEngine.runExtract().join();
        assertEquals(MerlinDataExchangeStatus.COMPLETE_SUCCESS, status);
    }

    @Test
    void testRunExtractWithBadTemplate() throws IOException
    {
        String username = ResourceAccess.getUsername();
        char[] password = ResourceAccess.getPassword();
        Path mockXml = getMockXml("merlin_mock_config_dx_bad_template.xml");
        List<Path> mocks = Collections.singletonList(mockXml);
        Path testDirectory = getTestDirectory();
        Instant start = Instant.parse("2019-01-01T08:00:00Z");
        Instant end = Instant.parse("2022-08-30T08:00:00Z");
        StoreOptionImpl storeOption = new StoreOptionImpl();
        storeOption.setRegular("0-replace-all");
        storeOption.setIrregular("0-delete_insert");
        MerlinParameters params = new MerlinParametersBuilder()
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
                .withParameters(params)
                .withProgressListener(buildLoggingProgressListener())
                .build();
        MerlinDataExchangeStatus status = dataExchangeEngine.runExtract().join();
        assertEquals(MerlinDataExchangeStatus.FAILURE, status);
    }

    @Test
    void testRunExtractWithPartialComplete() throws IOException
    {
        String username = ResourceAccess.getUsername();
        char[] password = ResourceAccess.getPassword();
        Path mockXml = getMockXml("merlin_mock_dx_partial_complete_multi_timestep.xml");
        List<Path> mocks = Collections.singletonList(mockXml);
        Path testDirectory = getTestDirectory();
        Instant start = Instant.parse("2019-01-01T08:00:00Z");
        Instant end = Instant.parse("2022-08-30T08:00:00Z");
        StoreOptionImpl storeOption = new StoreOptionImpl();
        storeOption.setRegular("0-replace-all");
        storeOption.setIrregular("0-delete_insert");
        MerlinParameters params = new MerlinParametersBuilder()
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
                .withParameters(params)
                .withProgressListener(buildLoggingProgressListener())
                .build();
        MerlinDataExchangeStatus status = dataExchangeEngine.runExtract().join();
        assertEquals(MerlinDataExchangeStatus.PARTIAL_SUCCESS, status);
    }

    @Test
    void testRunExtractCancelled() throws IOException, InterruptedException
    {
        String username = ResourceAccess.getUsername();
        char[] password = ResourceAccess.getPassword();
        Path mockXml = getMockXml("merlin_mock_config_dx.xml");
        Path testDirectory = getTestDirectory();
        Instant start = Instant.parse("2019-01-01T08:00:00Z");
        Instant end = Instant.parse("2022-08-30T08:00:00Z");
        StoreOptionImpl storeOption = new StoreOptionImpl();
        storeOption.setRegular("0-replace-all");
        storeOption.setIrregular("0-delete_insert");
        MerlinParameters params = new MerlinParametersBuilder()
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
        TestLogProgressListener progressListener = buildLoggingProgressListener();
        DataExchangeEngine dataExchangeEngine = new MerlinDataExchangeEngineBuilder()
                .withConfigurationFiles(Collections.singletonList(mockXml))
                .withParameters(params)
                .withProgressListener(progressListener)
                .build();
        dataExchangeEngine.runExtract();
        Thread.sleep(2000);
        dataExchangeEngine.cancelExtract();
        Thread.sleep(5000);
        assertTrue(progressListener.getProgress() < 100);
    }

    @Test
    void testNulls() throws IOException {
        String username = ResourceAccess.getUsername();
        char[] password = ResourceAccess.getPassword();
        Path mockXml = getMockXml("merlin_mock_config_dx.xml");
        Path testDirectory = getTestDirectory();
        Instant start = Instant.parse("2019-01-01T08:00:00Z");
        Instant end = Instant.parse("2022-08-30T08:00:00Z");
        StoreOptionImpl storeOption = new StoreOptionImpl();
        storeOption.setRegular("0-replace-all");
        storeOption.setIrregular("0-delete_insert");
        MerlinParameters params = new MerlinParametersBuilder()
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
        TestLogProgressListener progressListener = buildLoggingProgressListener();
        assertThrows(IllegalArgumentException.class, () -> new MerlinDataExchangeEngineBuilder()
                .withConfigurationFiles(null)
                .withParameters(params)
                .withProgressListener(progressListener)
                .build());
        assertThrows(IllegalArgumentException.class, () -> new MerlinDataExchangeEngineBuilder()
                .withConfigurationFiles(new ArrayList<>())
                .withParameters(params)
                .withProgressListener(progressListener)
                .build());
        assertThrows(NullPointerException.class, () -> new MerlinDataExchangeEngineBuilder()
                .withConfigurationFiles(Collections.singletonList(mockXml))
                .withParameters(null)
                .withProgressListener(progressListener)
                .build());
        assertThrows(NullPointerException.class, () -> new MerlinDataExchangeEngineBuilder()
                .withConfigurationFiles(Collections.singletonList(mockXml))
                .withParameters(params)
                .withProgressListener(null)
                .build());
    }


    private TestLogProgressListener buildLoggingProgressListener() throws IOException
    {
        return new TestLogProgressListener();
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

    private Path getTestDirectory()
    {
        return Paths.get(System.getProperty("user.dir")).resolve("build/tmp");
    }
}
