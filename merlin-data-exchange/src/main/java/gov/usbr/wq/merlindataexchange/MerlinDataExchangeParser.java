package gov.usbr.wq.merlindataexchange;

import com.fasterxml.jackson.dataformat.xml.JacksonXmlModule;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import gov.usbr.wq.merlindataexchange.configuration.DataExchangeConfiguration;

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
            return retVal;
        }
        catch (IOException | XMLStreamException e)
        {
            throw new MerlinConfigParseException(configFilepath, e);
        }
    }
}
