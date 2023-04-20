package gov.usbr.wq.merlindataexchange;

import gov.usbr.wq.merlindataexchange.configuration.DataStore;

import java.nio.file.Path;
import java.util.List;

final class MerlinConfigInvalidTypesException extends MerlinConfigParseException
{
    public MerlinConfigInvalidTypesException(Path configFile, DataStore destDataStore, String attemptedType, List<String> supportedTypes)
    {
        super(configFile, "DataExchange set of type " + attemptedType
                + " does not support writing to " + destDataStore.getDataStoreType() + ". Supported types: " + String.join(",", supportedTypes));
    }
}
