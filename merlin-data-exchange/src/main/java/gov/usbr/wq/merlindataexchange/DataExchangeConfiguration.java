package gov.usbr.wq.merlindataexchange;

import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlProperty;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlRootElement;

import java.util.List;

@JacksonXmlRootElement(namespace = "https://www.w3.org/2001/XMLSchema-instance", localName = "data-exchange-configuration")
final class DataExchangeConfiguration
{
    @JacksonXmlProperty(localName = "datastore")
    private List<DataStore> _dataStores;
    @JacksonXmlProperty(localName = "data-exchange-set")
    private List<DataExchangeSet> _dataExchangeSet;

    List<DataStore> getDataStores()
    {
        return _dataStores;
    }

    void setDataStoresMerlin(List<DataStore> dataStores)
    {
        _dataStores = dataStores;
    }

    List<DataExchangeSet> getDataExchangeSets()
    {
        return _dataExchangeSet;
    }

    void setDataExchangeSets(List<DataExchangeSet> dataExchangeSet)
    {
        _dataExchangeSet = dataExchangeSet;
    }

}
