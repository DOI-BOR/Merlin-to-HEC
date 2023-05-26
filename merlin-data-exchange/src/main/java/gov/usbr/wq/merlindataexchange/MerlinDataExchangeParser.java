package gov.usbr.wq.merlindataexchange;

import com.fasterxml.jackson.dataformat.xml.JacksonXmlModule;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import gov.usbr.wq.merlindataexchange.configuration.DataExchangeConfiguration;
import gov.usbr.wq.merlindataexchange.configuration.DataExchangeSet;
import gov.usbr.wq.merlindataexchange.configuration.DataStore;
import gov.usbr.wq.merlindataexchange.configuration.DataStoreProfile;
import gov.usbr.wq.merlindataexchange.configuration.DataStoreRef;
import gov.usbr.wq.merlindataexchange.io.DssDataExchangeWriter;
import gov.usbr.wq.merlindataexchange.io.MerlinDataExchangeTimeSeriesReader;
import gov.usbr.wq.merlindataexchange.io.wq.CsvProfileWriter;
import gov.usbr.wq.merlindataexchange.io.wq.MerlinDataExchangeProfileReader;
import hec.heclib.util.Unit;
import org.w3c.dom.Document;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.logging.Level;
import java.util.logging.Logger;

public final class MerlinDataExchangeParser
{

    private static final Logger LOGGER = Logger.getLogger(MerlinDataExchangeParser.class.getName());
    private MerlinDataExchangeParser()
    {
        throw new AssertionError("Utility class for parsing. Do not instantiate.");
    }

    public static DataExchangeConfiguration parseXmlFile(Path configFilepath) throws MerlinConfigParseException
    {
        try
        {
            validateConfigIsXml(configFilepath);
            int numberOfDataStores = countNumberOfElements(configFilepath, DataExchangeConfiguration.DATASTORE_ELEM);
            int numberOfProfileDataStores = countNumberOfElements(configFilepath, DataExchangeConfiguration.DATASTORE_PROFILE_ELEM);
            int numberOfSets = countNumberOfElements(configFilepath, DataExchangeConfiguration.DATA_EXCHANGE_SET_ELEM);
            XMLInputFactory factory = XMLInputFactory.newFactory();
            XMLStreamReader streamReader = factory.createXMLStreamReader(Files.newInputStream(configFilepath));
            JacksonXmlModule module = new JacksonXmlModule();
            module.setDefaultUseWrapper(false);
            XmlMapper xmlMapper = new XmlMapper(module);
            streamReader.next(); // to point to <root>
            DataExchangeConfiguration retVal = xmlMapper.readValue(streamReader, DataExchangeConfiguration.class);
            if(retVal.getDataExchangeSets().isEmpty())
            {
                throw new MerlinConfigParseException(configFilepath, "Missing data exchange set(s)");
            }
            if(retVal.getDataStores().isEmpty())
            {
                throw new MerlinConfigParseException(configFilepath, "Missing data stores");
            }
            int deserializedNumOfProfileDataStores = (int) retVal.getDataStores().stream().filter(DataStoreProfile.class::isInstance)
                    .count();
            int deserializedNumOfNonProfileDataStores = (int) retVal.getDataStores().stream().filter(ds -> !(ds instanceof DataStoreProfile))
                    .count();
            if(numberOfDataStores > deserializedNumOfNonProfileDataStores)
            {
                throw new MerlinConfigParseException(configFilepath, "All datastore(s) must be grouped together in a continuous list.");
            }
            if(numberOfProfileDataStores > deserializedNumOfProfileDataStores)
            {
                throw new MerlinConfigParseException(configFilepath, "All datastore-profile(s) must be grouped together in a continuous list.");
            }
            if(numberOfSets > retVal.getDataExchangeSets().size())
            {
                throw new MerlinConfigParseException(configFilepath, "All data exchange set(s) must be grouped together in a continuous list.");
            }
            validateParsedConfig(configFilepath, retVal);
            return retVal;
        }
        catch (IOException | XMLStreamException | SAXException | ParserConfigurationException e)
        {
            throw new MerlinConfigParseException(configFilepath, e);
        }
    }

    private static int countNumberOfElements(Path configFilepath, String elemName)
    {
        int count = 0;
        try
        {
            DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
            Document document = factory.newDocumentBuilder().parse(configFilepath.toString());

            // Create an XPath expression
            XPathFactory xPathFactory = XPathFactory.newInstance();
            XPath xPath = xPathFactory.newXPath();
            String expression = "count(//" + elemName + ")";
            count = (int) (double) xPath.evaluate(expression, document, XPathConstants.NUMBER);
        }
        catch (IOException | SAXException | ParserConfigurationException | XPathExpressionException e)
        {
            LOGGER.log(Level.CONFIG, e, () -> "Failed to read " + configFilepath);
        }
        return count;
    }

    private static void validateParsedConfig(Path configFilepath, DataExchangeConfiguration config) throws MerlinConfigParseException
    {
        List<DataStore> dataStores = config.getDataStores();
        for(DataStore dataStore : dataStores)
        {
            dataStore.validate(configFilepath);
        }
        for (DataExchangeSet set : config.getDataExchangeSets())
        {
            DataStoreRef dataStoreRefA = set.getDataStoreRefA();
            DataStoreRef dataStoreRefB = set.getDataStoreRefB();
            String type = set.getDataType();
            String templateName = set.getTemplateName();
            Integer templateId = set.getTemplateId();
            String qualityVersionName = set.getQualityVersionName();
            Integer qualityVersionId = set.getQualityVersionId();
            String unitSystem = set.getUnitSystem();
            String sourceId = set.getSourceId();
            List<String> validUnitSystems = Arrays.asList(Unit.getUnitSystems());
            if(type == null || type.trim().isEmpty())
            {
                throw new MerlinConfigParseException(configFilepath, "Missing type in data-exchange-set " + set.getId());
            }
            if((templateName == null || templateName.trim().isEmpty()) && templateId == null)
            {
                throw new MerlinConfigParseException(configFilepath, "Missing template in data-exchange-set " + set.getId());
            }
            if((qualityVersionName == null || qualityVersionName.trim().isEmpty()) && qualityVersionId == null)
            {
                throw new MerlinConfigParseException(configFilepath, "Missing quality-version in data-exchange-set " + set.getId());
            }
            if(!validUnitSystem(validUnitSystems, unitSystem))
            {
                throw new MerlinConfigParseException(configFilepath, "Unit System " + unitSystem + " does not match an accepted unit system: " + validUnitSystems);
            }
            if(sourceId == null || sourceId.trim().isEmpty())
            {
                throw new MerlinConfigParseException(configFilepath, "Missing source-id in data-exchange-set " + set.getId());
            }
            if(!sourceIdMatchesADataStore(sourceId, dataStores))
            {
                throw new MerlinConfigParseException(configFilepath, "source-id " + set.getSourceId() + " does not match any datastores in configuration file");
            }
            if(dataStoreRefA == null)
            {
                throw new MerlinConfigParseException(configFilepath, "Missing datastore-ref-a in data-exchange-set " + set.getId());
            }
            if(dataStoreRefB == null)
            {
                throw new MerlinConfigParseException(configFilepath, "Missing datastore-ref-b in data-exchange-set " + set.getId());
            }
            config.getDataStoreByRef(dataStoreRefA).orElseThrow(() -> new MerlinConfigParseException(configFilepath, "No data-store found for id: " + dataStoreRefA.getId()
                    + " in data-exchange-set " + set.getId()));
            config.getDataStoreByRef(dataStoreRefA).orElseThrow(() -> new MerlinConfigParseException(configFilepath, "No data-store found for id: " + dataStoreRefB.getId()
                    + " in data-exchange-set " + set.getId()));
            validateRefTypes(config, set, dataStoreRefA, dataStoreRefB, configFilepath);

        }
    }

    private static void validateRefTypes(DataExchangeConfiguration config, DataExchangeSet set, DataStoreRef dataStoreRefA, DataStoreRef dataStoreRefB, Path configFilepath)
            throws MerlinConfigParseException
    {
        DataStoreRef destRef = dataStoreRefA;
        if(set.getSourceId().equalsIgnoreCase(dataStoreRefA.getId()))
        {
            destRef = dataStoreRefB;
        }
        Optional<DataStore> dataStoreOpt = config.getDataStoreByRef(destRef);
        if(dataStoreOpt.isPresent())
        {
            DataStore dataStore = dataStoreOpt.get();
            if(set.getDataType().equalsIgnoreCase(MerlinDataExchangeTimeSeriesReader.TIMESERIES)
                    && !dataStore.getDataStoreType().equalsIgnoreCase(DssDataExchangeWriter.DSS))
            {
                throw new MerlinConfigInvalidTypesException(configFilepath, dataStore, MerlinDataExchangeTimeSeriesReader.TIMESERIES, Collections.singletonList(DssDataExchangeWriter.DSS));
            }
            if(set.getDataType().equalsIgnoreCase(MerlinDataExchangeProfileReader.PROFILE)
                    && !dataStore.getDataStoreType().equalsIgnoreCase(CsvProfileWriter.CSV))
            {
                throw new MerlinConfigInvalidTypesException(configFilepath, dataStore, MerlinDataExchangeProfileReader.PROFILE, Collections.singletonList(CsvProfileWriter.CSV));
            }
        }
    }

    private static boolean sourceIdMatchesADataStore(String sourceId, List<DataStore> dataStores)
    {
        boolean retVal = false;
        for(DataStore dataStore : dataStores)
        {
            if(dataStore.getId().equalsIgnoreCase(sourceId))
            {
                retVal = true;
                break;
            }
        }
        return retVal;
    }

    private static boolean validUnitSystem(List<String> validUnitSystems, String unitSystem)
    {
        boolean retVal = false;
        for(String validUnitSystem : validUnitSystems)
        {
            if(validUnitSystem.equalsIgnoreCase(unitSystem))
            {
                retVal = true;
                break;
            }
        }
        return retVal;
    }

    private static void validateConfigIsXml(Path configFilepath) throws IOException, SAXException, ParserConfigurationException
    {
        DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
        DocumentBuilder builder = factory.newDocumentBuilder();
        builder.parse(configFilepath.toFile());
    }
}
