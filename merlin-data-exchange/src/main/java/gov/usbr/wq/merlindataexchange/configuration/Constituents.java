package gov.usbr.wq.merlindataexchange.configuration;

import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlProperty;

import java.util.ArrayList;
import java.util.List;

public final class Constituents
{
    @JacksonXmlProperty(localName = "constituent")
    private final List<Constituent> _constituents = new ArrayList<>();

    public List<Constituent> getConstituents()
    {
        return new ArrayList<>(_constituents);
    }

    public void setConstituents(List<Constituent> constituents)
    {
        _constituents.clear();
        _constituents.addAll(constituents);
    }
}
