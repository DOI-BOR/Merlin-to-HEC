package gov.usbr.wq.merlindataexchange.configuration;

import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlProperty;
import gov.usbr.wq.merlindataexchange.MerlinConfigParseException;

import java.nio.file.Path;

public class DataStore
{
    @JacksonXmlProperty(isAttribute = true, localName = "id")
    private String _id;
    @JacksonXmlProperty(localName = "datastore-type")
    private String _dataStoreType;

    @JacksonXmlProperty(localName = "path")
    private String _path;

    public String getId()
    {
        return _id;
    }

    public String getDataStoreType()
    {
        return _dataStoreType;
    }

    public String getPath()
    {
        return _path;
    }

    public void validate(Path configFilepath) throws MerlinConfigParseException
    {
        if(getId() == null || getId().isEmpty())
        {
            throw new MerlinConfigParseException(configFilepath, "Missing id for datastore");
        }
        if(getDataStoreType() == null || getDataStoreType().trim().isEmpty())
        {
            throw new MerlinConfigParseException(configFilepath, "Missing data-type for datastore " + getId());
        }
        if(getPath() == null || getPath().trim().isEmpty())
        {
            throw new MerlinConfigParseException(configFilepath, "Missing path for datastore " + getId());
        }
    }
}
