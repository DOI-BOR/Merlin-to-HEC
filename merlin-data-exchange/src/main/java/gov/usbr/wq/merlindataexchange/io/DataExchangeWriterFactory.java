package gov.usbr.wq.merlindataexchange.io;

import gov.usbr.wq.merlindataexchange.configuration.DataStore;
import rma.util.lookup.Lookup;
import rma.util.lookup.Lookups;

public final class DataExchangeWriterFactory
{
    public static DataExchangeWriter lookupWriter(DataStore destination) throws DataExchangeLookupException
    {
        String delimeter = "/";
        String lookupPath = DataExchangeWriter.LOOKUP_PATH + delimeter + destination.getDataStoreType();
        Lookup lookup = Lookups.forPath(lookupPath);
        DataExchangeWriter retVal = lookup.lookup(DataExchangeWriter.class);
        if(retVal == null)
        {
            throw new DataExchangeLookupException(destination);
        }
        return retVal;
    }
}
