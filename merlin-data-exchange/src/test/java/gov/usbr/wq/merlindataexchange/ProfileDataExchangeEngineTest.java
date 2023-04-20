package gov.usbr.wq.merlindataexchange;

import gov.usbr.wq.merlindataexchange.parameters.AuthenticationParametersBuilder;
import gov.usbr.wq.merlindataexchange.parameters.MerlinParameters;
import gov.usbr.wq.merlindataexchange.parameters.MerlinParametersBuilder;
import hec.io.impl.StoreOptionImpl;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

final class ProfileDataExchangeEngineTest
{
    @Test
    void testCSVProfileExchange() throws IOException
    {
        String username = ResourceAccess.getUsername();
        char[] password = ResourceAccess.getPassword();
        String mockFileName = "merlin_mock_config_profile.xml";
        Path mockXml = getMockXml(mockFileName);
        List<Path> mocks = Collections.singletonList(mockXml);
        Path testDirectory = getTestDirectory();
        Path csvFile = testDirectory.resolve(mockFileName.replace(".xml", ".csv"));
        Files.deleteIfExists(csvFile);
        Instant start = ZonedDateTime.parse("2009-09-15T10:06:00-08:00", DateTimeFormatter.ISO_OFFSET_DATE_TIME).toInstant();
        Instant end = Instant.parse("2018-02-21T12:00:00Z");
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
    void testCSVMultipleProfilesInTemplateExchange() throws IOException
    {
        String username = ResourceAccess.getUsername();
        char[] password = ResourceAccess.getPassword();
        String mockFileName = "merlin_mock_config_profiles.xml";
        Path mockXml = getMockXml(mockFileName);
        List<Path> mocks = Collections.singletonList(mockXml);
        Path testDirectory = getTestDirectory();
        Path csvFile = testDirectory.resolve(mockFileName.replace(".xml", ".csv"));
        Files.deleteIfExists(csvFile);
        Instant start = ZonedDateTime.parse("2009-09-15T10:06:00-08:00", DateTimeFormatter.ISO_OFFSET_DATE_TIME).toInstant();
        Instant end = Instant.parse("2018-02-21T12:00:00Z");
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
    void testCSVProfileExchangeNoConstituents() throws IOException
    {
        String username = ResourceAccess.getUsername();
        char[] password = ResourceAccess.getPassword();
        String mockFileName = "merlin_mock_config_profile_no_constituents.xml";
        Path mockXml = getMockXml(mockFileName);
        List<Path> mocks = Collections.singletonList(mockXml);
        Path testDirectory = getTestDirectory();
        Path csvFile = testDirectory.resolve(mockFileName.replace(".xml", ".csv"));
        Files.deleteIfExists(csvFile);
        Instant start = ZonedDateTime.parse("2009-09-15T10:06:00-08:00", DateTimeFormatter.ISO_OFFSET_DATE_TIME).toInstant();
        Instant end = Instant.parse("2018-02-21T12:00:00Z");
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
    void testCSVProfileExchangeNoConstituentsNoUnitSystem() throws IOException
    {
        String username = ResourceAccess.getUsername();
        char[] password = ResourceAccess.getPassword();
        String mockFileName = "merlin_mock_config_profile_no_constituents_no_unit_system.xml";
        Path mockXml = getMockXml(mockFileName);
        List<Path> mocks = Collections.singletonList(mockXml);
        Path testDirectory = getTestDirectory();
        Path csvFile = testDirectory.resolve(mockFileName.replace(".xml", ".csv"));
        Files.deleteIfExists(csvFile);
        Instant start = ZonedDateTime.parse("2009-09-15T10:06:00-08:00", DateTimeFormatter.ISO_OFFSET_DATE_TIME).toInstant();
        Instant end = Instant.parse("2018-02-21T12:00:00Z");
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
