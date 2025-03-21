package gov.usbr.wq.merlindataexchange.configuration;

import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlProperty;

public class DataStoreRef
{
    @JacksonXmlProperty(isAttribute = true, localName = "id")
    private String _id;

    public String getId()
    {
        return _id;
    }

}
