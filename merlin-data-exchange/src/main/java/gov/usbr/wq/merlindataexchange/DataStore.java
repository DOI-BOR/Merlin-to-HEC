package gov.usbr.wq.merlindataexchange;

import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlProperty;

class DataStore
{
    @JacksonXmlProperty(isAttribute = true, localName = "id")
    private String _id;
    @JacksonXmlProperty(localName = "datastore-type")
    private String _dataStoreType;

    @JacksonXmlProperty(localName = "path")
    private String _path;

    String getId()
    {
        return _id;
    }

    void setId(String id)
    {
        this._id = id;
    }

    String getDataStoreType()
    {
        return _dataStoreType;
    }

    void setDataStoreType(String dataStoreType)
    {
        _dataStoreType = dataStoreType;
    }

    String getPath()
    {
        return _path;
    }

    void setPath(String path)
    {
        _path = path;
    }
}
