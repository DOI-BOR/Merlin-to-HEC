package gov.usbr.wq.merlindataexchange.io;

import gov.usbr.wq.merlindataexchange.configuration.DataStore;
import rma.util.lookup.Lookup;
import rma.util.lookup.Lookups;

import java.util.logging.Logger;

public final class DataExchangeWriterFactory
{
    public static DataExchangeWriter lookupWriter(DataStore destination, Logger logFileLogger) throws DataExchangeLookupException
    {
        String delimeter = "/";
        String lookupPath = DataExchangeWriter.LOOKUP_PATH + delimeter + destination.getDataStoreType();
        logFileLogger.info(() -> "Looking for DataExchangeWriter at: " + lookupPath);
        Lookup lookup = Lookups.forPath(lookupPath);
        DataExchangeWriter retVal = lookup.lookup(DataExchangeWriter.class);
        if(retVal == null)
        {
            throw new DataExchangeLookupException("Failed to look up DataExchangeWrite using lookup path: " + lookupPath);
        }
        return retVal;
    }
}
