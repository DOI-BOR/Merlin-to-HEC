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
import gov.usbr.wq.merlindataexchange.parameters.AuthenticationParametersBuilder;
import gov.usbr.wq.merlindataexchange.parameters.MerlinParameters;
import gov.usbr.wq.merlindataexchange.parameters.MerlinParametersBuilder;
import hec.heclib.dss.DSSPathname;
import hec.heclib.dss.HecTimeSeriesBase;
import hec.heclib.util.HecTime;
import hec.io.DSSIdentifier;
import hec.io.TimeSeriesContainer;
import hec.io.impl.StoreOptionImpl;
import hec.lang.Const;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import javax.xml.stream.XMLStreamException;
import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.DecimalFormat;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.TimeZone;

import static org.junit.jupiter.api.Assertions.*;

final class MerlinDataExchangeEngineTest
{

    @Test
    void testRunExtract() throws IOException, XMLStreamException, HttpAccessException
    {
        String username = ResourceAccess.getUsername();
        char[] password = ResourceAccess.getPassword();
        String mockFileName = "merlin_mock_config_dx.xml";
        Path mockXml = getMockXml(mockFileName);
        //Path mockXml2 = getMockXml("merlin_mock_config_dx_skip_all.xml");
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
        Map<String, DataWrapper> expectedDssToData = buildExpectedDss(mocks, start, end, username, password);
        assertNotNull(expectedDssToData);
        String dssFileName = testDirectory.resolve(mockFileName.replace(".xml", ".dss")).toString();
        for(Map.Entry<String, DataWrapper> entry : expectedDssToData.entrySet())
        {
            String dssPath = entry.getKey();
            DataWrapper merlinData = entry.getValue();
            TimeSeriesContainer tsc = DssFileManagerImpl.getDssFileManager().readTS(new DSSIdentifier(dssFileName, dssPath), false);
            assertNotNull(tsc);
            assertEquals(merlinData.getEvents().size(), tsc.getNumberValues());
            DSSPathname pathname = new DSSPathname(tsc.getFullName());
            assertEquals( HecTimeSeriesBase.getEPartFromInterval(Integer.parseInt(merlinData.getTimestep())), pathname.ePart());
            assertEquals(merlinData.getParameter(), pathname.cPart());
            assertEquals(merlinData.getStation() + "-" + merlinData.getSensor(), pathname.getBPart());
            assertEquals(merlinData.getProject(), pathname.getAPart());
            int i=0;
            for(EventWrapper event : merlinData.getEvents())
            {
                HecTime merlinTimeZulu = HecTime.fromZonedDateTime(event.getDate());
                HecTime tscTimeZulu = tsc.getTimes().elementAt(i);
                tscTimeZulu = HecTime.convertToTimeZone(tscTimeZulu, TimeZone.getTimeZone("GMT-8"), TimeZone.getTimeZone("Z"));
                assertEquals(event.getValue(), tsc.getValue(i), 1.0E-14);
                assertEquals(merlinTimeZulu.date(), tscTimeZulu.date());
                i++;
            }
        }
    }

    @Test
    void testRunExtractLargeTimeWindow() throws IOException, XMLStreamException, HttpAccessException
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
        String dssFileName = testDirectory.resolve(mockFileName.replace(".xml", ".dss")).toString();
        for(Map.Entry<String, DataWrapper> entry : expectedDssToData.entrySet())
        {
            String dssPath = entry.getKey();
            DataWrapper merlinData = entry.getValue();
            TimeSeriesContainer tsc = DssFileManagerImpl.getDssFileManager().readTS(new DSSIdentifier(dssFileName, dssPath), false);
            assertNotNull(tsc);
            DSSPathname pathname = new DSSPathname(tsc.getFullName());
            assertEquals( HecTimeSeriesBase.getEPartFromInterval(Integer.parseInt(merlinData.getTimestep())), pathname.ePart());
            assertEquals(merlinData.getParameter(), pathname.cPart());
            assertEquals(merlinData.getStation() + "-" + merlinData.getSensor(), pathname.getBPart());
            assertEquals(merlinData.getProject(), pathname.getAPart());
            NavigableSet<EventWrapper> events = merlinData.getEvents();
            int i = 0;
            int expectedNumTrimmed = getExpectedNumberOfTrimmedValues(events);
            int lastIndex = events.size() - expectedNumTrimmed;
            for(EventWrapper event : events)
            {
                double value = Const.UNDEFINED_DOUBLE;
                if(event.getValue() != null)
                {
                    value = event.getValue();
                }
                if(i <= lastIndex)
                {
                    HecTime merlinTimeZulu = HecTime.fromZonedDateTime(event.getDate());
                    HecTime tscTimeZulu = tsc.getTimes().elementAt(i);
                    tscTimeZulu = HecTime.convertToTimeZone(tscTimeZulu, TimeZone.getTimeZone("GMT-8"), TimeZone.getTimeZone("Z"));
                    assertEquals(value, tsc.getValue(i), 1.0E-4);
                    assertEquals(merlinTimeZulu.date(), tscTimeZulu.date());
                }
                i++;
            }
        }
    }

    private int getExpectedNumberOfTrimmedValues(NavigableSet<EventWrapper> events)
    {
        int missingCount = 0;
        for(EventWrapper event : events)
        {
            if(event.getValue() == null && event.getQuality() == 0)
            {
                missingCount ++;
            }
            else
            {
                missingCount = 0;
            }
        }
        return missingCount;
    }

    private Map<String, DataWrapper> buildExpectedDss(List<Path> mocks, Instant start, Instant end, String username, char[] pw) throws XMLStreamException, IOException, HttpAccessException {
        ApiConnectionInfo connectionInfo = new ApiConnectionInfo("https://www.grabdata2.com");
        TokenContainer token = HttpAccessUtils.authenticate(connectionInfo, username, pw);
        MerlinTimeSeriesDataAccess access = new MerlinTimeSeriesDataAccess();
        Map<String, DataWrapper> retVal = new HashMap<>();
        for (Path mock : mocks) {
            DataExchangeConfiguration config = MerlinDataExchangeParser.parseXmlFile(mock);
            for (DataExchangeSet set : config.getDataExchangeSets()) {
                int templateId = set.getTemplateId();
                TemplateWrapper templateWrapper = new TemplateWrapperBuilder().withDprID(templateId).build();
                List<MeasureWrapper> measures = access.getMeasurementsByTemplate(connectionInfo, token, templateWrapper);
                for (MeasureWrapper measure : measures) {
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
        return retVal;
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

    @Test
    void testBadUsernamePassword() throws IOException
    {
        String username = "TheCookieMonster";
        char[] password = "NotARealPassword".toCharArray();
        String mockFileName = "merlin_mock_config_dx.xml";
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
        assertEquals(MerlinDataExchangeStatus.FAILURE, status);
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

    private Path getTestDirectory()
    {
        return Paths.get(System.getProperty("user.dir")).resolve("build/tmp");
    }
}
