package gov.usbr.wq.merlindataexchange.io;

import gov.usbr.wq.merlindataexchange.MerlinDataExchangeParameters;
import gov.usbr.wq.merlindataexchange.configuration.DataStore;

public interface DataExchanger
{
    void initialize(DataStore dataStore, MerlinDataExchangeParameters parameters);
    void close();
}
