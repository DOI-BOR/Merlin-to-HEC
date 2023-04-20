package gov.usbr.wq.merlindataexchange.io;

import gov.usbr.wq.merlindataexchange.configuration.DataExchangeSet;
import gov.usbr.wq.merlindataexchange.configuration.DataStore;
import rma.util.lookup.Lookup;
import rma.util.lookup.Lookups;

public final class DataExchangeWriterFactory
{
    private DataExchangeWriterFactory()
    {
        throw new AssertionError("Factory Class");
    }

    public static DataExchangeWriter<?> lookupWriter(DataStore destination, DataExchangeSet set) throws DataExchangeLookupException
    {
        String delimiter = "/";
        String lookupPath = DataExchangeWriter.LOOKUP_PATH + delimiter + set.getDataType() + delimiter + destination.getDataStoreType();
        Lookup lookup = Lookups.forPath(lookupPath);
        DataExchangeWriter<?> retVal = lookup.lookup(DataExchangeWriter.class);
        if(retVal == null)
        {
            throw new DataExchangeLookupException(destination);
        }
        return retVal;
    }
}
