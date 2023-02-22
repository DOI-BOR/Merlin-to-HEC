package gov.usbr.wq.merlindataexchange.io;

import gov.usbr.wq.merlindataexchange.configuration.DataStore;
import rma.util.lookup.Lookup;
import rma.util.lookup.Lookups;

import java.util.logging.Logger;

public final class DataExchangeReaderFactory
{
    public static DataExchangeReader lookupReader(DataStore source, Logger logFileLogger) throws DataExchangeLookupException
    {
        String delimeter = "/";
        String lookupPath = DataExchangeReader.LOOKUP_PATH + delimeter + source.getDataStoreType();
        logFileLogger.info(() -> "Looking for DataExchangeReader at: " + lookupPath);
        Lookup lookup = Lookups.forPath(lookupPath);
        DataExchangeReader retVal = lookup.lookup(DataExchangeReader.class);
        if(retVal == null)
        {
            throw new DataExchangeLookupException("Failed to look up DataExchangeReader using lookup path: " + lookupPath);
        }
        return retVal;
    }
}
