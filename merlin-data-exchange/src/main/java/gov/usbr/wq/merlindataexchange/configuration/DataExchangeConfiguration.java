package gov.usbr.wq.merlindataexchange.configuration;

import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlProperty;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlRootElement;

import java.util.List;
import java.util.Optional;

@JacksonXmlRootElement(namespace = "https://www.w3.org/2001/XMLSchema-instance", localName = "data-exchange-configuration")
public final class DataExchangeConfiguration
{
    @JacksonXmlProperty(localName = "datastore")
    private List<DataStore> _dataStores;
    @JacksonXmlProperty(localName = "data-exchange-set")
    private List<DataExchangeSet> _dataExchangeSet;

    public List<DataStore> getDataStores()
    {
        return _dataStores;
    }

    public void setDataStoresMerlin(List<DataStore> dataStores)
    {
        _dataStores = dataStores;
    }

    public List<DataExchangeSet> getDataExchangeSets()
    {
        return _dataExchangeSet;
    }

    public void setDataExchangeSets(List<DataExchangeSet> dataExchangeSet)
    {
        _dataExchangeSet = dataExchangeSet;
    }

    public Optional<DataStore> getDataStoreByRef(DataStoreRef ref)
    {
        return getDataStores().stream()
                .filter(ds -> ds.getId().equalsIgnoreCase(ref.getId()))
                .findFirst();
    }
}
