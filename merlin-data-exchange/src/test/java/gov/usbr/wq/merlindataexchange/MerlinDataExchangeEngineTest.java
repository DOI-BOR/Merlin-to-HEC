package gov.usbr.wq.merlindataexchange;

import com.rma.io.DssFileManagerImpl;
import gov.usbr.wq.dataaccess.MerlinTimeSeriesDataAccess;
import gov.usbr.wq.dataaccess.http.ApiConnectionInfo;
import gov.usbr.wq.dataaccess.http.HttpAccessException;
import gov.usbr.wq.dataaccess.http.HttpAccessUtils;
import gov.usbr.wq.dataaccess.http.TokenContainer;
import gov.usbr.wq.dataaccess.model.DataWrapper;
import gov.usbr.wq.dataaccess.model.EventWrapper;
import gov.usbr.wq.dataaccess.model.MeasureWrapper;
import gov.usbr.wq.dataaccess.model.TemplateWrapper;
import gov.usbr.wq.dataaccess.model.TemplateWrapperBuilder;
import gov.usbr.wq.merlindataexchange.configuration.DataExchangeConfiguration;
import gov.usbr.wq.merlindataexchange.configuration.DataExchangeSet;
import gov.usbr.wq.merlindataexchange.io.DssDataExchangeWriter;
import gov.usbr.wq.merlindataexchange.parameters.AuthenticationParametersBuilder;
import gov.usbr.wq.merlindataexchange.parameters.MerlinParameters;
import gov.usbr.wq.merlindataexchange.parameters.MerlinParametersBuilder;
import hec.data.DataSetIllegalArgumentException;
import hec.data.Interval;
import hec.data.IntervalOffset;
import hec.data.Units;
import hec.data.UnitsConversionException;
import hec.heclib.dss.DSSPathname;
import hec.heclib.dss.HecTimeSeriesBase;
import hec.heclib.util.HecTime;
import hec.io.DSSIdentifier;
import hec.io.TimeSeriesContainer;
import hec.io.impl.StoreOptionImpl;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.TimeZone;
import java.util.TreeMap;

import static org.junit.jupiter.api.Assertions.*;

final class MerlinDataExchangeEngineTest
{

    @Test
    void testRunExtract() throws IOException, HttpAccessException, MerlinConfigParseException, UnitsConversionException
    {
        String username = ResourceAccess.getUsername();
        char[] password = ResourceAccess.getPassword();
        String mockFileName = "merlin_mock_config_dx.xml";
        Path mockXml = getMockXml(mockFileName);
        List<Path> mocks = Arrays.asList(mockXml);
        Path testDirectory = getTestDirectory();
        Path dssFile = testDirectory.resolve(mockFileName.replace(".xml", ".dss"));
        Instant start = Instant.parse("2003-02-01T12:00:00Z");
        Instant end = Instant.parse("2022-02-21T12:00:00Z");
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
        Map<String, DataWrapper> expectedDssToData = buildExpectedDss(mocks, start, end, username, password);
        assertNotNull(expectedDssToData);
        verifyData(expectedDssToData, testDirectory, mockFileName);
    }

    @Test
    void testRunExtractUnsupportedQualityVersion() throws IOException, HttpAccessException, MerlinConfigParseException, UnitsConversionException
    {
        String username = ResourceAccess.getUsername();
        char[] password = ResourceAccess.getPassword();
        String mockFileName = "merlin_mock_config_unsupported_quality_version_dx.xml";
        Path mockXml = getMockXml(mockFileName);
        List<Path> mocks = Arrays.asList(mockXml);
        Path testDirectory = getTestDirectory();
        Path dssFile = testDirectory.resolve(mockFileName.replace(".xml", ".dss"));
        if(Files.exists(dssFile))
        {
            Files.delete(dssFile);
        }
        Instant start = Instant.parse("2016-02-01T12:00:00Z");
        Instant end = Instant.parse("2016-02-21T12:00:00Z");
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
    void testRunExtractMultiThreadedOn() throws IOException, HttpAccessException, MerlinConfigParseException, UnitsConversionException
    {
        System.setProperty(DssDataExchangeWriter.MERLIN_TO_DSS_WRITE_SINGLE_THREAD_PROPERTY_KEY, "True");
        String username = ResourceAccess.getUsername();
        char[] password = ResourceAccess.getPassword();
        String mockFileName = "merlin_mock_config_dx.xml";
        Path mockXml = getMockXml(mockFileName);
        List<Path> mocks = Arrays.asList(mockXml);
        Path testDirectory = getTestDirectory();
        Path dssFile = testDirectory.resolve(mockFileName.replace(".xml", ".dss"));
        Instant start = Instant.parse("2003-02-01T12:00:00Z");
        Instant end = Instant.parse("2022-02-21T12:00:00Z");
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
        Map<String, DataWrapper> expectedDssToData = buildExpectedDss(mocks, start, end, username, password);
        assertNotNull(expectedDssToData);
        verifyData(expectedDssToData, testDirectory, mockFileName);
    }

    @Test
    void testRunExtractMultiThreadedOff() throws IOException, HttpAccessException, MerlinConfigParseException, UnitsConversionException
    {
        System.setProperty(DssDataExchangeWriter.MERLIN_TO_DSS_WRITE_SINGLE_THREAD_PROPERTY_KEY, "False");
        String username = ResourceAccess.getUsername();
        char[] password = ResourceAccess.getPassword();
        String mockFileName = "merlin_mock_config_dx.xml";
        Path mockXml = getMockXml(mockFileName);
        List<Path> mocks = Arrays.asList(mockXml);
        Path testDirectory = getTestDirectory();
        Path dssFile = testDirectory.resolve(mockFileName.replace(".xml", ".dss"));
        Instant start = Instant.parse("2003-02-01T12:00:00Z");
        Instant end = Instant.parse("2022-02-21T12:00:00Z");
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
        Map<String, DataWrapper> expectedDssToData = buildExpectedDss(mocks, start, end, username, password);
        assertNotNull(expectedDssToData);
        verifyData(expectedDssToData, testDirectory, mockFileName);
    }

    @Test
    void testRunExtractMultiThreadedNotSet() throws IOException, HttpAccessException, MerlinConfigParseException, UnitsConversionException
    {
        String username = ResourceAccess.getUsername();
        char[] password = ResourceAccess.getPassword();
        String mockFileName = "merlin_mock_config_dx.xml";
        Path mockXml = getMockXml(mockFileName);
        List<Path> mocks = Arrays.asList(mockXml);
        Path testDirectory = getTestDirectory();
        Path dssFile = testDirectory.resolve(mockFileName.replace(".xml", ".dss"));
        if(Files.exists(dssFile))
        {
            Files.delete(dssFile);
        }
        Instant start = Instant.parse("2003-02-01T12:00:00Z");
        Instant end = Instant.parse("2022-02-21T12:00:00Z");
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
        Map<String, DataWrapper> expectedDssToData = buildExpectedDss(mocks, start, end, username, password);
        assertNotNull(expectedDssToData);
        verifyData(expectedDssToData, testDirectory, mockFileName);
    }

    @Test
    void testRunExtractPartial() throws IOException, HttpAccessException, MerlinConfigParseException, UnitsConversionException
    {
        String username = ResourceAccess.getUsername();
        char[] password = ResourceAccess.getPassword();
        String mockFileName = "merlin_mock_config_partial_dx.xml";
        Path mockXml = getMockXml(mockFileName);
        List<Path> mocks = Arrays.asList(mockXml);
        Path testDirectory = getTestDirectory();
        Path dssFile = testDirectory.resolve(mockFileName.replace(".xml", ".dss"));
        if(Files.exists(dssFile))
        {
            Files.delete(dssFile);
        }
        Instant start = Instant.parse("2016-02-01T12:00:00Z");
        Instant end = Instant.parse("2016-02-21T12:00:00Z");
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
        Map<String, DataWrapper> expectedDssToData = buildExpectedDss(mocks, start, end, username, password);
        assertNotNull(expectedDssToData);
        verifyData(expectedDssToData, testDirectory, mockFileName);
    }

    @Test
    void testRunExtractLargeTimeWindow() throws IOException, HttpAccessException, MerlinConfigParseException, UnitsConversionException
    {
        String username = ResourceAccess.getUsername();
        char[] password = ResourceAccess.getPassword();
        String mockFileName = "merlin_mock_config_dx_test_large_window.xml";
        Path mockXml = getMockXml(mockFileName);
        List<Path> mocks = Arrays.asList(mockXml);
        Path testDirectory = getTestDirectory();
        Path dssFile = testDirectory.resolve(mockFileName.replace(".xml", ".dss"));
        if(Files.exists(dssFile))
        {
            Files.delete(dssFile);
        }
        Instant start = Instant.parse("2015-02-01T12:00:00Z");
        Instant end = Instant.parse("2022-08-21T12:00:00Z");
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
        Map<String, DataWrapper> expectedDssToData = buildExpectedDss(mocks, start, end, username, password);
        assertNotNull(expectedDssToData);
        verifyData(expectedDssToData, testDirectory, mockFileName);
    }

    @Test
    void testRunExtractNeedsInterpolation() throws IOException, HttpAccessException, MerlinConfigParseException, UnitsConversionException
    {
        String username = ResourceAccess.getUsername();
        char[] password = ResourceAccess.getPassword();
        String mockFileName = "merlin_mock_config_needs_interpolation.xml";
        Path mockXml = getMockXml(mockFileName);
        List<Path> mocks = Arrays.asList(mockXml);
        Path testDirectory = getTestDirectory();
        Path dssFile = testDirectory.resolve(mockFileName.replace(".xml", ".dss"));
        if(Files.exists(dssFile))
        {
            Files.delete(dssFile);
        }
        Instant start = Instant.parse("2015-02-01T12:00:00Z");
        Instant end = Instant.parse("2022-08-21T12:00:00Z");
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
        Map<String, DataWrapper> expectedDssToData = buildExpectedDss(mocks, start, end, username, password);
        assertNotNull(expectedDssToData);
        verifyData(expectedDssToData, testDirectory, mockFileName);
    }

    private Map<String, DataWrapper> buildExpectedDss(List<Path> mocks, Instant start, Instant end, String username, char[] pw) throws IOException, HttpAccessException, MerlinConfigParseException
    {
        ApiConnectionInfo connectionInfo = new ApiConnectionInfo("https://www.grabdata2.com");
        TokenContainer token = HttpAccessUtils.authenticate(connectionInfo, username, pw);
        MerlinTimeSeriesDataAccess access = new MerlinTimeSeriesDataAccess();
        List<TemplateWrapper> templates = access.getTemplates(connectionInfo, token);
        Map<String, DataWrapper> retVal = new HashMap<>();
        for (Path mock : mocks)
        {
            DataExchangeConfiguration config = MerlinDataExchangeParser.parseXmlFile(mock);
            for (DataExchangeSet set : config.getDataExchangeSets())
            {
                Integer templateId = set.getTemplateId();
                if(templateId == null)
                {
                    templateId = templates.stream().filter(t -> t.getName().equalsIgnoreCase(set.getTemplateName()))
                            .findFirst().orElse(new TemplateWrapper(null)).getDprId();
                }
                Integer id = templateId;
                if(templateId != null && templates.stream().anyMatch(t -> Objects.equals(t.getDprId(), id)))
                {
                    TemplateWrapper templateWrapper = new TemplateWrapperBuilder().withDprID(templateId).build();
                    List<MeasureWrapper> measures = access.getMeasurementsByTemplate(connectionInfo, token, templateWrapper);
                    for (MeasureWrapper measure : measures)
                    {
                        DataWrapper data = access.getEventsBySeries(connectionInfo, token, measure, 1,
                                start, end);
                        String timeStep = data.getTimestep();
                        if (timeStep != null && !timeStep.contains(",") && !data.getSeriesId().isEmpty() && !data.getEvents().isEmpty())
                        {
                            String range = "";
                            int parsedInterval = Integer.parseInt(data.getTimestep());
                            String interval = HecTimeSeriesBase.getEPartFromInterval(parsedInterval);
                            retVal.put("/" + data.getProject() + "/" + data.getStation() + "-" + data.getSensor() + "/" +
                                    data.getParameter() + "/" + range + "/" + interval + "/fPart/", data);
                        }
                    }
                }
            }
        }
        return retVal;
    }

    @Test
    void testRunExtractWithNoTemplateIdButHasName() throws IOException, HttpAccessException, MerlinConfigParseException, UnitsConversionException
    {
        String username = ResourceAccess.getUsername();
        char[] password = ResourceAccess.getPassword();
        String mockFileName = "merlin_mock_config_dx_missing_template_id.xml";
        Path mockXml = getMockXml(mockFileName);
        List<Path> mocks = Arrays.asList(mockXml);
        Path testDirectory = getTestDirectory();
        Path dssFile = testDirectory.resolve(mockFileName.replace(".xml", ".dss"));
        if(Files.exists(dssFile))
        {
            Files.delete(dssFile);
        }
        Instant start = Instant.parse("2016-02-01T12:00:00Z");
        Instant end = Instant.parse("2016-02-21T12:00:00Z");
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
        List<Path> originalConfigMock = Collections.singletonList(getMockXml("merlin_mock_config_partial_dx.xml"));
        Map<String, DataWrapper> expectedDssToData = buildExpectedDss(originalConfigMock, start, end, username, password);
        assertNotNull(expectedDssToData);
        verifyData(expectedDssToData, testDirectory, mockFileName);
    }

    @Test
    void testRunExtractWithNoTemplateNameButHasId() throws IOException, HttpAccessException, MerlinConfigParseException, UnitsConversionException
    {
        String username = ResourceAccess.getUsername();
        char[] password = ResourceAccess.getPassword();
        String mockFileName = "merlin_mock_config_dx_missing_template_name.xml";
        Path mockXml = getMockXml(mockFileName);
        List<Path> mocks = Arrays.asList(mockXml);
        Path testDirectory = getTestDirectory();
        Path dssFile = testDirectory.resolve(mockFileName.replace(".xml", ".dss"));
        if(Files.exists(dssFile))
        {
            Files.delete(dssFile);
        }
        Instant start = Instant.parse("2016-02-01T12:00:00Z");
        Instant end = Instant.parse("2016-02-21T12:00:00Z");
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
        List<Path> originalConfigMock = Collections.singletonList(getMockXml("merlin_mock_config_partial_dx.xml"));
        Map<String, DataWrapper> expectedDssToData = buildExpectedDss(originalConfigMock, start, end, username, password);
        assertNotNull(expectedDssToData);
        verifyData(expectedDssToData, testDirectory, mockFileName);
    }

    @Test
    void testRunExtractWithNoQVIdButHasName() throws IOException, HttpAccessException, MerlinConfigParseException, UnitsConversionException
    {
        String username = ResourceAccess.getUsername();
        char[] password = ResourceAccess.getPassword();
        String mockFileName = "merlin_mock_config_dx_missing_qv_id.xml";
        Path mockXml = getMockXml(mockFileName);
        List<Path> mocks = Arrays.asList(mockXml);
        Path testDirectory = getTestDirectory();
        Path dssFile = testDirectory.resolve(mockFileName.replace(".xml", ".dss"));
        if(Files.exists(dssFile))
        {
            Files.delete(dssFile);
        }
        Instant start = Instant.parse("2016-02-01T12:00:00Z");
        Instant end = Instant.parse("2016-02-21T12:00:00Z");
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
        List<Path> originalConfigMock = Collections.singletonList(getMockXml("merlin_mock_config_partial_dx.xml"));
        Map<String, DataWrapper> expectedDssToData = buildExpectedDss(originalConfigMock, start, end, username, password);
        assertNotNull(expectedDssToData);
        verifyData(expectedDssToData, testDirectory, mockFileName);
    }

    @Test
    void testRunExtractWithNoQVNameButHasId() throws IOException, HttpAccessException, MerlinConfigParseException, UnitsConversionException
    {
        String username = ResourceAccess.getUsername();
        char[] password = ResourceAccess.getPassword();
        String mockFileName = "merlin_mock_config_dx_missing_qv_name.xml";
        Path mockXml = getMockXml(mockFileName);
        List<Path> mocks = Arrays.asList(mockXml);
        Path testDirectory = getTestDirectory();
        Path dssFile = testDirectory.resolve(mockFileName.replace(".xml", ".dss"));
        if(Files.exists(dssFile))
        {
            Files.delete(dssFile);
        }
        Instant start = Instant.parse("2016-02-01T12:00:00Z");
        Instant end = Instant.parse("2016-02-21T12:00:00Z");
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
        List<Path> originalConfigMock = Collections.singletonList(getMockXml("merlin_mock_config_partial_dx.xml"));
        Map<String, DataWrapper> expectedDssToData = buildExpectedDss(originalConfigMock, start, end, username, password);
        assertNotNull(expectedDssToData);
        verifyData(expectedDssToData, testDirectory, mockFileName);
    }

    private void verifyData(Map<String, DataWrapper> expectedDssToData, Path testDirectory, String mockFileName) throws UnitsConversionException
    {
        String dssFileName = testDirectory.resolve(mockFileName.replace(".xml", ".dss")).toString();
        for(Map.Entry<String, DataWrapper> entry : expectedDssToData.entrySet())
        {
            String dssPath = entry.getKey();
            DataWrapper merlinData = entry.getValue();
            TimeSeriesContainer tsc = DssFileManagerImpl.getDssFileManager().readTS(new DSSIdentifier(dssFileName, dssPath), false);
            assertNotNull(tsc);
            boolean interpolationNeeded = false;
            try {
                int expectedNumValues = merlinData.getEvents().size();
                interpolationNeeded = calculateInterpolationNeeded(false, merlinData.getStartTime(), merlinData.getEndTime(), merlinData.getTimestep(), merlinData.getTimeZone(), merlinData.getEvents().size());
                if (interpolationNeeded)
                {
                    ZonedDateTime startTime = merlinData.getStartTime();
                    ZonedDateTime endTime = merlinData.getEndTime();
                    int parsedInterval = Integer.parseInt(merlinData.getTimestep());
                    String interval = HecTimeSeriesBase.getEPartFromInterval(parsedInterval);
                    int offsetMinutes = calculateOffsetInMinutes(startTime, new Interval(interval), TimeZone.getTimeZone(merlinData.getTimeZone()));
                    expectedNumValues = calculateNumberOfExpectedIntervals(startTime, endTime, offsetMinutes, parsedInterval, interval, merlinData.getTimeZone()) + 1;
                }
                if(merlinData.getEvents().stream().noneMatch(e -> e.getValue() == null))
                {
                    assertEquals(expectedNumValues, tsc.getNumberValues());
                }
            }
            catch (DataSetIllegalArgumentException e)
            {
                throw new RuntimeException(e);
            }
            DSSPathname pathname = new DSSPathname(tsc.getFullName());
            assertEquals( HecTimeSeriesBase.getEPartFromInterval(Integer.parseInt(merlinData.getTimestep())), pathname.ePart());
            assertEquals(merlinData.getParameter(), pathname.cPart());
            assertEquals(merlinData.getStation() + "-" + merlinData.getSensor(), pathname.getBPart());
            assertEquals(merlinData.getProject(), pathname.getAPart());
            NavigableMap<HecTime, EventWrapper> eventMap = new TreeMap<>();
            for(EventWrapper event : merlinData.getEvents())
            {
                HecTime merlinTimeZulu = HecTime.fromZonedDateTime(event.getDate());
                eventMap.put(merlinTimeZulu, event);
            }
            for(int i=0; i < tsc.getNumberValues(); i++)
            {
                HecTime tscTimeZulu = tsc.getTimes().elementAt(i);
                tscTimeZulu = HecTime.convertToTimeZone(tscTimeZulu, TimeZone.getTimeZone("GMT-8"), TimeZone.getTimeZone("Z"));
                double tscVal = Units.convertUnits(tsc.getValue(i), tsc.units, merlinData.getUnits());
                EventWrapper event = eventMap.get(tscTimeZulu);
                if(!interpolationNeeded)
                {
                    assertNotNull(event);
                }
                if(event != null && event.getValue() != null)
                {
                    assertEquals(event.getValue(), tscVal, 1.0E-4);
                }
            }

        }
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
    void testRunExtractWith2BadTemplates() throws IOException
    {
        String username = ResourceAccess.getUsername();
        char[] password = ResourceAccess.getPassword();
        Path mockXml = getMockXml("merlin_mock_config_dx_2_bad_templates.xml");
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
    void testRunExtractWithBadConfigParse() throws IOException
    {
        String username = ResourceAccess.getUsername();
        char[] password = ResourceAccess.getPassword();
        Path mockXml = getMockXml("invalidxmls/merlin_mock_bad_config.xml");
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
    void testRunExtractWithMissingDataSetsParse() throws IOException
    {
        String username = ResourceAccess.getUsername();
        char[] password = ResourceAccess.getPassword();
        Path mockXml = getMockXml("invalidxmls/merlin_mock_missing_datasets.xml");
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
    void testRunExtractWithMissingDataStoreParse() throws IOException
    {
        String username = ResourceAccess.getUsername();
        char[] password = ResourceAccess.getPassword();
        Path mockXml = getMockXml("invalidxmls/merlin_mock_missing_datastores.xml");
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
        Path mockXml = getMockXml("merlin_mock_config_partial_dx.xml");
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
        Path mockXml = getMockXml("merlin_mock_config_partial_dx.xml");
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

    @Test
    void testBadUsernamePassword() throws IOException
    {
        String username = "TheCookieMonster";
        char[] password = "NotARealPassword".toCharArray();
        String mockFileName = "merlin_mock_config_partial_dx.xml";
        Path mockXml = getMockXml(mockFileName);
        List<Path> mocks = Arrays.asList(mockXml);
        Path testDirectory = getTestDirectory();
        Instant start = Instant.parse("2016-02-01T12:00:00Z");
        Instant end = Instant.parse("2016-02-21T12:00:00Z");
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
        //this will become authentication failure status once fix to return status code 401 is in
        assertEquals(MerlinDataExchangeStatus.AUTHENTICATION_FAILURE, status);
    }

    @Test
    void testNonExistentWebServer() throws IOException
    {
        String username = ResourceAccess.getUsername();
        char[] password = ResourceAccess.getPassword();
        String mockFileName = "merlin_mock_config_dx_non_existent_server.xml";
        Path mockXml = getMockXml(mockFileName);
        List<Path> mocks = Arrays.asList(mockXml);
        Path testDirectory = getTestDirectory();
        Instant start = Instant.parse("2016-02-01T12:00:00Z");
        Instant end = Instant.parse("2016-02-21T12:00:00Z");
        StoreOptionImpl storeOption = new StoreOptionImpl();
        storeOption.setRegular("0-replace-all");
        storeOption.setIrregular("0-delete_insert");
        MerlinParameters params = new MerlinParametersBuilder()
                .withWatershedDirectory(testDirectory)
                .withLogFileDirectory(testDirectory)
                .withAuthenticationParameters(new AuthenticationParametersBuilder()
                        .forUrl("https://www.grabcookiemonsterdata2.com")
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
        assertEquals(MerlinDataExchangeStatus.AUTHENTICATION_FAILURE, status);
    }

    @Test
    void testWebServerWithBadEndpoint() throws IOException
    {
        String username = ResourceAccess.getUsername();
        char[] password = ResourceAccess.getPassword();
        String mockFileName = "merlin_mock_config_dx_bad_endpoint.xml";
        Path mockXml = getMockXml(mockFileName);
        List<Path> mocks = Arrays.asList(mockXml);
        Path testDirectory = getTestDirectory();
        Instant start = Instant.parse("2016-02-01T12:00:00Z");
        Instant end = Instant.parse("2016-02-21T12:00:00Z");
        StoreOptionImpl storeOption = new StoreOptionImpl();
        storeOption.setRegular("0-replace-all");
        storeOption.setIrregular("0-delete_insert");
        MerlinParameters params = new MerlinParametersBuilder()
                .withWatershedDirectory(testDirectory)
                .withLogFileDirectory(testDirectory)
                .withAuthenticationParameters(new AuthenticationParametersBuilder()
                        .forUrl("https://www.grabdata2.com/badendpoint")
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
        assertEquals(MerlinDataExchangeStatus.AUTHENTICATION_FAILURE, status);
    }

    @Test
    void testMismatchedAuthenticationUrlFromWhatsInConfig() throws IOException
    {
        String username = ResourceAccess.getUsername();
        char[] password = ResourceAccess.getPassword();
        String mockFileName = "merlin_mock_config_dx_bad_endpoint.xml";
        Path mockXml = getMockXml(mockFileName);
        List<Path> mocks = Arrays.asList(mockXml);
        Path testDirectory = getTestDirectory();
        Instant start = Instant.parse("2016-02-01T12:00:00Z");
        Instant end = Instant.parse("2016-02-21T12:00:00Z");
        StoreOptionImpl storeOption = new StoreOptionImpl();
        storeOption.setRegular("0-replace-all");
        storeOption.setIrregular("0-delete_insert");
        MerlinParameters params = new MerlinParametersBuilder()
                .withWatershedDirectory(testDirectory)
                .withLogFileDirectory(testDirectory)
                .withAuthenticationParameters(new AuthenticationParametersBuilder()
                        //config doesn't have this url, so should get a mismatch error and failure
                        .forUrl("https://www.grabdata2.com/")
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

    private static int calculateOffsetInMinutes(ZonedDateTime start, Interval interval, TimeZone timeZone) throws DataSetIllegalArgumentException
    {
        Instant prevInterval = Instant.ofEpochMilli(Interval.getPreviousIntervalTime(start.toInstant().toEpochMilli(), interval, timeZone));
        Instant interValToCheck = Instant.ofEpochMilli(Interval.getNextIntervalTime(prevInterval.toEpochMilli(), interval, timeZone));
        return (int) Duration.between(interValToCheck, start.toInstant()).toMinutes();
    }

    private static boolean calculateInterpolationNeeded(boolean isProcessed, ZonedDateTime startTime, ZonedDateTime endTime, String timeStep, ZoneId dataZoneId, int numberOfEvents)
            throws DataSetIllegalArgumentException
    {
        boolean retVal = !isProcessed;
        if(!isProcessed)
        {
            int parsedInterval = Integer.parseInt(timeStep);
            String interval = HecTimeSeriesBase.getEPartFromInterval(parsedInterval);
            int offsetMinutes = calculateOffsetInMinutes(startTime, new Interval(interval), TimeZone.getTimeZone(dataZoneId));
            int numIntervals = calculateNumberOfExpectedIntervals(startTime, endTime, offsetMinutes, parsedInterval, interval, dataZoneId);
            retVal = numIntervals + 1 != numberOfEvents;
        }
        return retVal;
    }

    private static int calculateNumberOfExpectedIntervals(ZonedDateTime startTime, ZonedDateTime endTime, int offsetMinutes, int parsedInterval, String interval, ZoneId dataZoneId)
            throws DataSetIllegalArgumentException
    {
        IntervalOffset offset = new IntervalOffset(offsetMinutes/60, parsedInterval/60);
        return (int) Interval.calcNumberOfIntervals(Date.from(startTime.toInstant()),
                Date.from(endTime.toInstant()), new Interval(interval), offset, TimeZone.getTimeZone(dataZoneId));
    }

    private Path getTestDirectory()
    {
        return Paths.get(System.getProperty("user.dir")).resolve("build/tmp");
    }
}
