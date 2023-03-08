package gov.usbr.wq.merlindataexchange;

import com.fasterxml.jackson.dataformat.xml.JacksonXmlModule;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import gov.usbr.wq.merlindataexchange.configuration.DataExchangeConfiguration;
import gov.usbr.wq.merlindataexchange.configuration.DataExchangeSet;
import gov.usbr.wq.merlindataexchange.configuration.DataStore;
import gov.usbr.wq.merlindataexchange.configuration.DataStoreRef;
import org.xml.sax.SAXException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

public final class MerlinDataExchangeParser
{
    private MerlinDataExchangeParser()
    {
        throw new AssertionError("Utility class for parsing. Do not instantiate.");
    }

    public static DataExchangeConfiguration parseXmlFile(Path configFilepath) throws MerlinConfigParseException
    {
        try
        {
            validateConfigIsXml(configFilepath);
            XMLInputFactory factory = XMLInputFactory.newFactory();
            XMLStreamReader streamReader = factory.createXMLStreamReader(Files.newInputStream(configFilepath));
            JacksonXmlModule module = new JacksonXmlModule();
            module.setDefaultUseWrapper(false);
            XmlMapper xmlMapper = new XmlMapper(module);
            streamReader.next(); // to point to <root>
            DataExchangeConfiguration retVal = xmlMapper.readValue(streamReader, DataExchangeConfiguration.class);
            if(retVal.getDataExchangeSets() == null)
            {
                throw new MerlinConfigParseException(configFilepath, "Missing data exchange set(s)");
            }
            if(retVal.getDataStores() == null)
            {
                throw new MerlinConfigParseException(configFilepath, "Missing data stores");
            }
            validateParsedConfig(configFilepath, retVal);
            return retVal;
        }
        catch (IOException | XMLStreamException | SAXException | ParserConfigurationException e)
        {
            throw new MerlinConfigParseException(configFilepath, e);
        }
    }

    private static void validateParsedConfig(Path configFilepath, DataExchangeConfiguration config) throws MerlinConfigParseException
    {
        for (DataExchangeSet set : config.getDataExchangeSets())
        {
            DataStoreRef dataStoreRefA = set.getDataStoreRefA();
            DataStoreRef dataStoreRefB = set.getDataStoreRefB();
            if(dataStoreRefA == null)
            {
                throw new MerlinConfigParseException(configFilepath, "Missing datastore-ref-a in data-exchange-set " + set.getId());
            }
            if(dataStoreRefB == null)
            {
                throw new MerlinConfigParseException(configFilepath, "Missing datastore-ref-b in data-exchange-set " + set.getId());
            }
            DataStore dataStoreA = config.getDataStoreByRef(dataStoreRefA).orElseThrow(() -> new MerlinConfigParseException(configFilepath, "No data-store found for id: " + dataStoreRefA.getId()
                    + " in data-exchange-set " + set.getId()));
            DataStore dataStoreB = config.getDataStoreByRef(dataStoreRefA).orElseThrow(() -> new MerlinConfigParseException(configFilepath, "No data-store found for id: " + dataStoreRefB.getId()
                    + " in data-exchange-set " + set.getId()));
            validateDataStore(configFilepath, dataStoreA);
            validateDataStore(configFilepath, dataStoreB);
        }
    }

    private static void validateDataStore(Path configFilepath, DataStore dataStore) throws MerlinConfigParseException
    {
        if(dataStore.getDataStoreType() == null || dataStore.getDataStoreType().trim().isEmpty())
        {
            throw new MerlinConfigParseException(configFilepath, "Missing data-type for datastore " + dataStore.getId());
        }
        if(dataStore.getPath() == null || dataStore.getPath().trim().isEmpty())
        {
            throw new MerlinConfigParseException(configFilepath, "Missing path for datastore " + dataStore.getId());
        }
    }

    private static void validateConfigIsXml(Path configFilepath) throws IOException, SAXException, ParserConfigurationException
    {
        DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
        DocumentBuilder builder = factory.newDocumentBuilder();
        builder.parse(configFilepath.toFile());
    }
}
