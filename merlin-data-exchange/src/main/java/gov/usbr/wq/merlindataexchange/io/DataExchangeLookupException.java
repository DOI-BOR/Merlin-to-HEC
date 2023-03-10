package gov.usbr.wq.merlindataexchange.io;

import gov.usbr.wq.merlindataexchange.configuration.DataStore;
import rma.util.lookup.Lookup;
import rma.util.lookup.Lookups;

import java.util.ArrayList;
import java.util.List;
import java.util.ServiceLoader;

public final class DataExchangeLookupException extends Exception
{

    public DataExchangeLookupException(DataStore dataStore)
    {
        super("Unsupported datastore-type: " + dataStore.getDataStoreType());
    }

}
