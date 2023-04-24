package gov.usbr.wq.merlindataexchange.parameters;

import gov.usbr.wq.merlindataexchange.MerlinDataExchangeLogger;
import gov.usbr.wq.merlindataexchange.configuration.DataExchangeSet;

import java.nio.file.Path;
import java.time.Instant;
import java.util.List;

import static gov.usbr.wq.merlindataexchange.io.wq.MerlinDataExchangeProfileReader.PROFILE;

public final class MerlinProfileParameters extends MerlinParameters
{
    MerlinProfileParameters(Path watershedDirectory, Path logFileDirectory, Instant start, Instant end, List<AuthenticationParameters> authenticationParameters)
    {
        super(watershedDirectory, logFileDirectory, start, end, authenticationParameters);
    }

    @Override
    public void logAdditionalParameters(MerlinDataExchangeLogger logBody)
    {
        //no additional parameters to log
    }

    @Override
    public boolean supportsDataExchangeSet(DataExchangeSet dataExchangeSet)
    {
        return dataExchangeSet.getDataType().equalsIgnoreCase(PROFILE);
    }
}
