package gov.usbr.wq.merlindataexchange.configuration;

import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlProperty;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlRootElement;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

@JacksonXmlRootElement(namespace = "https://www.w3.org/2001/XMLSchema-instance", localName = "data-exchange-configuration")
public final class DataExchangeConfiguration
{
    public static final String DATASTORE_ELEM = "datastore";
    public static final String DATASTORE_PROFILE_ELEM = "datastore-profile";
    public static final String DATA_EXCHANGE_SET_ELEM = "data-exchange-set";
    @JacksonXmlProperty(localName = DATASTORE_ELEM)
    private final List<DataStore> _dataStores = new ArrayList<>();
    @JacksonXmlProperty(localName = DATASTORE_PROFILE_ELEM)
    private final List<DataStoreProfile> _dataStoreProfiles = new ArrayList<>();
    @JacksonXmlProperty(localName = DATA_EXCHANGE_SET_ELEM)
    private final List<DataExchangeSet> _dataExchangeSet = new ArrayList<>();

    public List<DataStore> getDataStores()
    {
        List<DataStore> dataStores = new ArrayList<>();
        dataStores.addAll(_dataStores);
        dataStores.addAll(_dataStoreProfiles);
        return dataStores;
    }

    void setDataStoreProfiles(List<DataStoreProfile> dataStoreProfiles)
    {
        _dataStoreProfiles.clear();
        _dataStoreProfiles.addAll(dataStoreProfiles);
    }

    public void setDataStores(List<DataStore> dataStores)
    {
        _dataStores.clear();
        _dataStores.addAll(dataStores);
    }

    public List<DataExchangeSet> getDataExchangeSets()
    {
        return new ArrayList<>(_dataExchangeSet);
    }

    public void setDataExchangeSets(List<DataExchangeSet> dataExchangeSet)
    {
        _dataExchangeSet.clear();
        _dataExchangeSet.addAll(dataExchangeSet);
    }

    public Optional<DataStore> getDataStoreByRef(DataStoreRef ref)
    {
        return getDataStores().stream()
                .filter(ds -> ds.getId().equalsIgnoreCase(ref.getId()))
                .findFirst();
    }

    public void removeDataExchangeSet(DataExchangeSet set)
    {
        _dataExchangeSet.remove(set);
    }
}
