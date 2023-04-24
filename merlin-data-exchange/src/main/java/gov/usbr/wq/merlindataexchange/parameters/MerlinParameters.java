package gov.usbr.wq.merlindataexchange.parameters;


import gov.usbr.wq.merlindataexchange.MerlinDataExchangeLogger;
import gov.usbr.wq.merlindataexchange.configuration.DataExchangeSet;

import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

public abstract class MerlinParameters
{
    private final Instant _start;
    private final Instant _end;
    private final Path _watershedDirectory;
    private final Path _logFileDirectory;
    private final List<AuthenticationParameters> _authenticationParameters;

    MerlinParameters(Path watershedDirectory, Path logFileDirectory, Instant start, Instant end, List<AuthenticationParameters> authenticationParameters)
    {
        _authenticationParameters = authenticationParameters;
        _watershedDirectory = watershedDirectory;
        _logFileDirectory = logFileDirectory;
        _start = start;
        _end = end;
    }

    public Instant getStart()
    {
        return _start;
    }

    public Instant getEnd()
    {
        return _end;
    }

    public Path getWatershedDirectory()
    {
        return _watershedDirectory;
    }

    public Path getLogFileDirectory()
    {
        return _logFileDirectory;
    }

    public UsernamePasswordHolder getUsernamePasswordForUrl(String url) throws UsernamePasswordNotFoundException
    {
        return _authenticationParameters.stream()
                .filter(authParam -> authParam.getUrl().equalsIgnoreCase(url))
                .findFirst()
                .orElseThrow(() -> new UsernamePasswordNotFoundException(url))
                .getUsernamePassword();
    }

    public List<AuthenticationParameters> getAuthenticationParameters()
    {
        return new ArrayList<>(_authenticationParameters);
    }

    public abstract void logAdditionalParameters(MerlinDataExchangeLogger logBody);

    public abstract boolean supportsDataExchangeSet(DataExchangeSet dataExchangeSet);
}
