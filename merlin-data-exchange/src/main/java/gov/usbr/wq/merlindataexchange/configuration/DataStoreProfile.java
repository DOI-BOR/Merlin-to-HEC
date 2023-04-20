package gov.usbr.wq.merlindataexchange.configuration;

import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlProperty;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlRootElement;
import gov.usbr.wq.merlindataexchange.MerlinConfigParseException;

import java.nio.file.Path;
import java.util.List;


@JacksonXmlRootElement(localName = "datastore-profile")
public final class DataStoreProfile extends DataStore
{
    public static final String DEPTH = "Depth";
    @JacksonXmlProperty(localName = "constituents")
    private Constituents _constituents = new Constituents();

    public List<Constituent> getConstituents()
    {
        return _constituents.getConstituents();
    }

    public void setConstituents(Constituents constituents)
    {
        _constituents = constituents;
    }

    public Constituent getConstituentByParameter(String parameter)
    {
        Constituent retVal = null;
        for(Constituent constituent : _constituents.getConstituents())
        {
            if(constituent.getParameter().equalsIgnoreCase(parameter))
            {
                retVal = constituent;
                break;
            }
        }
        return retVal;
    }

    @Override
    public void validate(Path configFilepath) throws MerlinConfigParseException
    {
        super.validate(configFilepath);
        for(Constituent constituent : _constituents.getConstituents())
        {
            if(constituent.getParameter() == null)
            {
                throw new MerlinConfigParseException(configFilepath, "Constituent in data-store " + getId() + " missing parameter name");
            }
        }
    }
}
