package gov.usbr.wq.merlindataexchange.configuration;

import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlProperty;

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

    public void setId(String id)
    {
        this._id = id;
    }

    public String getDataStoreType()
    {
        return _dataStoreType;
    }

    public void setDataStoreType(String dataStoreType)
    {
        _dataStoreType = dataStoreType;
    }

    public String getPath()
    {
        return _path;
    }

    void setPath(String path)
    {
        _path = path;
    }
}
