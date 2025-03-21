package gov.usbr.wq.merlindataexchange.configuration;

import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlProperty;

public final class Constituent
{
    @JacksonXmlProperty(isAttribute = true, localName = "parameter")
    private String _parameter;

    @JacksonXmlProperty(isAttribute = true, localName = "unit")
    private String _unit;

    public String getParameter()
    {
        return _parameter;
    }

    public String getUnit()
    {
        return _unit;
    }

}
