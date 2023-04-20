package gov.usbr.wq.merlindataexchange.io.wq;

import gov.usbr.wq.merlindataexchange.DataExchangeEngine;
import gov.usbr.wq.merlindataexchange.MerlinDataExchangeEngineBuilder;
import gov.usbr.wq.merlindataexchange.MerlinDataExchangeStatus;
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
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Collections;
import java.util.List;
import java.util.SortedSet;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

final class ProfileTimeWindowCutoffTest
{
    @Test
    void testStartCutoff() throws IOException, InvalidProfileCsvException
    {
        String username = ResourceAccess.getUsername();
        char[] password = ResourceAccess.getPassword();
        String mockFileName = "merlin_mock_config_profile.xml";
        Path mockXml = getMockXml(mockFileName);
        List<Path> mocks = Collections.singletonList(mockXml);
        Path testDirectory = getTestDirectory();
        Path csvFile = testDirectory.resolve(mockFileName.replace(".xml", ".csv"));
        Files.deleteIfExists(csvFile);
        ZonedDateTime startDateTime = ZonedDateTime.parse("2009-09-15T10:06:00-08:00", DateTimeFormatter.ISO_OFFSET_DATE_TIME);
        Instant start = startDateTime.toInstant();
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
                .withProgressListener(new TestLogProgressListener())
                .build();
        MerlinDataExchangeStatus status = dataExchangeEngine.runExtract().join();
        assertEquals(MerlinDataExchangeStatus.COMPLETE_SUCCESS, status);
        SortedSet<ProfileSample> profiles = CsvProfileObjectMapper.deserializeDataFromCsv(csvFile);
        assertNotEquals(startDateTime, profiles.first().getDateTime());
    }

    @Test
    void testStartNotCutoff() throws IOException, InvalidProfileCsvException
    {
        String username = ResourceAccess.getUsername();
        char[] password = ResourceAccess.getPassword();
        String mockFileName = "merlin_mock_config_profile.xml";
        Path mockXml = getMockXml(mockFileName);
        List<Path> mocks = Collections.singletonList(mockXml);
        Path testDirectory = getTestDirectory();
        Path csvFile = testDirectory.resolve(mockFileName.replace(".xml", ".csv"));
        Files.deleteIfExists(csvFile);
        ZonedDateTime startDateTime = ZonedDateTime.parse("2009-09-15T09:58:00-08:00", DateTimeFormatter.ISO_OFFSET_DATE_TIME);
        Instant start = startDateTime.toInstant();
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
                .withProgressListener(new TestLogProgressListener())
                .build();
        MerlinDataExchangeStatus status = dataExchangeEngine.runExtract().join();
        assertEquals(MerlinDataExchangeStatus.COMPLETE_SUCCESS, status);
        SortedSet<ProfileSample> profiles = CsvProfileObjectMapper.deserializeDataFromCsv(csvFile);
        assertEquals(startDateTime, profiles.first().getDateTime());
    }

    @Test
    void testEndCutoff() throws IOException, InvalidProfileCsvException
    {
        String username = ResourceAccess.getUsername();
        char[] password = ResourceAccess.getPassword();
        String mockFileName = "merlin_mock_config_profile.xml";
        Path mockXml = getMockXml(mockFileName);
        List<Path> mocks = Collections.singletonList(mockXml);
        Path testDirectory = getTestDirectory();
        Path csvFile = testDirectory.resolve(mockFileName.replace(".xml", ".csv"));
        Files.deleteIfExists(csvFile);
        ZonedDateTime startDateTime = ZonedDateTime.parse("2009-07-15T10:06:00-08:00", DateTimeFormatter.ISO_OFFSET_DATE_TIME);
        ZonedDateTime endDateTime = ZonedDateTime.parse("2009-09-01T10:26:00-08:00", DateTimeFormatter.ISO_OFFSET_DATE_TIME);
        ZonedDateTime startDateTimeForLastProfile = ZonedDateTime.parse("2009-09-01T10:17:00-08:00", DateTimeFormatter.ISO_OFFSET_DATE_TIME);
        Instant start = startDateTime.toInstant();
        Instant end = endDateTime.toInstant();
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
                .withProgressListener(new TestLogProgressListener())
                .build();
        MerlinDataExchangeStatus status = dataExchangeEngine.runExtract().join();
        assertEquals(MerlinDataExchangeStatus.COMPLETE_SUCCESS, status);
        SortedSet<ProfileSample> profiles = CsvProfileObjectMapper.deserializeDataFromCsv(csvFile);
        assertNotEquals(startDateTimeForLastProfile, profiles.last().getDateTime());
    }

    @Test
    void testEndNotCutoff() throws IOException, InvalidProfileCsvException
    {
        String username = ResourceAccess.getUsername();
        char[] password = ResourceAccess.getPassword();
        String mockFileName = "merlin_mock_config_profile.xml";
        Path mockXml = getMockXml(mockFileName);
        List<Path> mocks = Collections.singletonList(mockXml);
        Path testDirectory = getTestDirectory();
        Path csvFile = testDirectory.resolve(mockFileName.replace(".xml", ".csv"));
        Files.deleteIfExists(csvFile);
        ZonedDateTime startDateTime = ZonedDateTime.parse("2009-07-15T10:06:00-08:00", DateTimeFormatter.ISO_OFFSET_DATE_TIME);
        ZonedDateTime endDateTime = ZonedDateTime.parse("2009-09-01T10:27:00-08:00", DateTimeFormatter.ISO_OFFSET_DATE_TIME);
        ZonedDateTime startDateTimeForLastProfile = ZonedDateTime.parse("2009-09-01T10:17:00-08:00", DateTimeFormatter.ISO_OFFSET_DATE_TIME);
        Instant start = startDateTime.toInstant();
        Instant end = endDateTime.toInstant();
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
                .withProgressListener(new TestLogProgressListener())
                .build();
        MerlinDataExchangeStatus status = dataExchangeEngine.runExtract().join();
        assertEquals(MerlinDataExchangeStatus.COMPLETE_SUCCESS, status);
        SortedSet<ProfileSample> profiles = CsvProfileObjectMapper.deserializeDataFromCsv(csvFile);
        assertEquals(startDateTimeForLastProfile, profiles.last().getDateTime());
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
