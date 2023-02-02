package gov.usbr.wq.merlindataexchange;

import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlProperty;

final class DataStoreLocalDss extends DataStore
{
    @JacksonXmlProperty(localName = "filepath")
    private String _filepath;

    String getFilepath()
    {
        return _filepath;
    }

    void setFilepath(String filepath)
    {
        _filepath = filepath;
    }
}
