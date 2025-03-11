package gov.usbr.wq.merlindataexchange;

import gov.usbr.wq.merlindataexchange.configuration.DataExchangeConfiguration;
import gov.usbr.wq.merlindataexchange.configuration.DataExchangeSet;
import gov.usbr.wq.merlindataexchange.configuration.DataStore;
import gov.usbr.wq.merlindataexchange.configuration.DataStoreRef;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toList;
import static org.junit.jupiter.api.Assertions.*;

final class MerlinDataExchangeParserTest
{
    @Test
    void testParseXmlFile() throws IOException, MerlinConfigParseException
    {
        Path mockXml = getMockXml("merlin_mock_dx_partial_complete_multi_timestep.xml");
        DataExchangeConfiguration dataExchangeConfig = MerlinDataExchangeParser.parseXmlFile(mockXml);
        assertNotNull(dataExchangeConfig);
        List<DataExchangeSet> tsDataExchangeSets = dataExchangeConfig.getDataExchangeSets();
        assertFalse(tsDataExchangeSets.isEmpty());
        DataExchangeSet tsDataExchangeSet1 = tsDataExchangeSets.get(0);
        DataExchangeSet tsDataExchangeSet2 = tsDataExchangeSets.get(1);
        DataStoreRef merlinRef1 = tsDataExchangeSet1.getDataStoreRefA();
        DataStoreRef localDssRef1 = tsDataExchangeSet1.getDataStoreRefB();
        DataStoreRef merlinRef2 = tsDataExchangeSet2.getDataStoreRefA();
        DataStoreRef localDssRef2 = tsDataExchangeSet2.getDataStoreRefB();

        assertEquals(2, dataExchangeConfig.getSupportedTimeSeriesTypes().size());
        assertTrue(dataExchangeConfig.getSupportedTimeSeriesTypes().contains("auto"));
        assertTrue(dataExchangeConfig.getSupportedTimeSeriesTypes().contains("step"));

        assertEquals(1, dataExchangeConfig.getSupportedProfileTypes().size());
        assertTrue(dataExchangeConfig.getSupportedProfileTypes().contains("profile"));

        List<DataStore> dataStores = dataExchangeConfig.getDataStores();
        assertNotNull(dataStores);

        DataStore dataStoreMerlin1 = dataStores.get(0);
        DataStore dataStoreLocalDss1 = dataStores.get(2);
        assertEquals("www.grabdata.com", dataStoreMerlin1.getId());
        assertEquals("https://www.grabdata.com", dataStoreMerlin1.getPath());
        assertEquals("wat", dataStoreLocalDss1.getId());
        assertEquals("$WATERSHED/merlin_mock_dx_partial_complete_multi_timestep.dss", dataStoreLocalDss1.getPath());
        assertEquals("Auburn Dam - Daily", tsDataExchangeSet1.getId());
        assertEquals("www.grabdata2.com", tsDataExchangeSet1.getSourceId());
        assertEquals(230, tsDataExchangeSet1.getTemplateId());
        assertEquals("Auburn Dam - Daily", tsDataExchangeSet1.getTemplateName());
        assertEquals(0, tsDataExchangeSet1.getQualityVersionId());
        assertEquals("All", tsDataExchangeSet1.getQualityVersionName());
        assertEquals("SI", tsDataExchangeSet1.getUnitSystem());
        assertEquals("time-series", tsDataExchangeSet1.getDataType());
        assertEquals("www.grabdata2.com", merlinRef1.getId());
        assertEquals("wat2", localDssRef1.getId());

        DataStore dataStoreMerlin2 = dataStores.get(1);
        DataStore dataStoreLocalDss2 = dataStores.get(3);
        assertEquals("www.grabdata2.com", dataStoreMerlin2.getId());
        assertEquals("https://www.grabdata2.com", dataStoreMerlin2.getPath());
        assertEquals("wat2", dataStoreLocalDss2.getId());
        assertEquals("$WATERSHED/wat2.dss", dataStoreLocalDss2.getPath());
        assertEquals("Folsom Lake - MR Boundary Flow", tsDataExchangeSet2.getId());
        assertEquals("www.grabdata2.com", tsDataExchangeSet2.getSourceId());
        assertEquals(231, tsDataExchangeSet2.getTemplateId());
        assertEquals("Folsom Lake - MR Boundary Flow", tsDataExchangeSet2.getTemplateName());
        assertEquals(0, tsDataExchangeSet2.getQualityVersionId());
        assertEquals("All", tsDataExchangeSet2.getQualityVersionName());
        assertEquals("SI", tsDataExchangeSet2.getUnitSystem());
        assertEquals("time-series", tsDataExchangeSet2.getDataType());
        assertEquals("www.grabdata2.com", merlinRef2.getId());
        assertEquals("wat2", localDssRef2.getId());
    }

    @Test
    void testInvalidXmls() throws IOException
    {
        try(Stream<Path> invalidXmlPath = Files.list(getInvalidMockXMlFolder()))
        {
            List<File> invalidXmlFiles = invalidXmlPath
                    .map(Path::toFile)
                    .filter(File::isFile)
                    .collect(toList());
            for (File file : invalidXmlFiles)
            {
                assertThrows(MerlinConfigParseException.class, () ->
                {
                    try
                    {
                        MerlinDataExchangeParser.parseXmlFile(file.toPath());
                    }
                    catch (MerlinConfigParseException e)
                    {
                        System.out.println(e.getMessage());
                        System.out.println("-------------------------------------------------------------------");
                        throw e;
                    }
                }, "Expected " + file.toString() + " to fail");
            }
        }
    }

    private Path getInvalidMockXMlFolder() throws IOException
    {
        String resource = "gov/usbr/wq/merlindataexchange/invalidxmls/";
        URL resourceUrl = getClass().getClassLoader().getResource(resource);
        if (resourceUrl == null)
        {
            throw new IOException("Failed to get resource: " + resource);
        }
        return new File(resourceUrl.getFile()).toPath();
    }

    private Path getMockXml(String xmlFileName) throws IOException
    {
        String resource = "gov/usbr/wq/merlindataexchange/" + xmlFileName;
        URL resourceUrl = getClass().getClassLoader().getResource(resource);
        if (resourceUrl == null)
        {
            throw new IOException("Failed to get resource: " + resource);
        }
        return new File(resourceUrl.getFile()).toPath();
    }
}
