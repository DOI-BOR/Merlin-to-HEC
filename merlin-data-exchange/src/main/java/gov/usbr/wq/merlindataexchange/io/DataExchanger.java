package gov.usbr.wq.merlindataexchange.io;

import gov.usbr.wq.merlindataexchange.parameters.MerlinParameters;
import gov.usbr.wq.merlindataexchange.configuration.DataStore;

public interface DataExchanger
{
    void initialize(DataStore dataStore, MerlinParameters parameters);
    void close();
}
