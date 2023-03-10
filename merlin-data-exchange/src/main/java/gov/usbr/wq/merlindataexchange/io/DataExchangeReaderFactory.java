package gov.usbr.wq.merlindataexchange.io;

import gov.usbr.wq.merlindataexchange.configuration.DataStore;
import rma.util.lookup.Lookup;
import rma.util.lookup.Lookups;

import java.util.ArrayList;
import java.util.List;
import java.util.ServiceLoader;

public final class DataExchangeReaderFactory
{
    public static DataExchangeReader lookupReader(DataStore source) throws DataExchangeLookupException
    {
        String delimeter = "/";
        String lookupPath = DataExchangeReader.LOOKUP_PATH + delimeter + source.getDataStoreType();
        Lookup lookup = Lookups.forPath(lookupPath);
        DataExchangeReader retVal = lookup.lookup(DataExchangeReader.class);
        if(retVal == null)
        {
            throw new DataExchangeLookupException(source);
        }
        return retVal;
    }
}
