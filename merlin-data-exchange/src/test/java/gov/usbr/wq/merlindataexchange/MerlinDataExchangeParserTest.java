package gov.usbr.wq.merlindataexchange;

import gov.usbr.wq.merlindataexchange.configuration.DataExchangeConfiguration;
import gov.usbr.wq.merlindataexchange.configuration.DataExchangeSet;
import gov.usbr.wq.merlindataexchange.configuration.DataStore;
import gov.usbr.wq.merlindataexchange.configuration.DataStoreRef;
import org.junit.jupiter.api.Test;

import javax.xml.stream.XMLStreamException;
import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

final class MerlinDataExchangeParserTest
{
    @Test
    void testParseXmlFile() throws IOException, XMLStreamException
    {
        Path mockXml = getMockXml();
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
        assertEquals(80, tsDataExchangeSet1.getTemplateId());
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
        assertEquals(83, tsDataExchangeSet2.getTemplateId());
        assertEquals("Folsom Lake - MR Boundary Flow", tsDataExchangeSet2.getTemplateName());
        assertEquals(0, tsDataExchangeSet2.getQualityVersionId());
        assertEquals("All", tsDataExchangeSet2.getQualityVersionName());
        assertEquals("SI", tsDataExchangeSet2.getUnitSystem());
        assertEquals("time-series", tsDataExchangeSet2.getDataType());
        assertEquals("www.grabdata2.com", merlinRef2.getId());
        assertEquals("wat2", localDssRef2.getId());
    }

    private Path getMockXml() throws IOException
    {
        String resource = "gov/usbr/wq/merlindataexchange/merlin_mock_dx_partial_complete_multi_timestep.xml";
        URL resourceUrl = getClass().getClassLoader().getResource(resource);
        if (resourceUrl == null)
        {
            throw new IOException("Failed to get resource: " + resource);
        }
        return new File(resourceUrl.getFile()).toPath();
    }
}
