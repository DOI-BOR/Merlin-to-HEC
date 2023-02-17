package gov.usbr.wq.merlindataexchange;

import hec.io.StoreOption;

import java.nio.file.Path;
import java.time.Instant;
import java.util.Arrays;

public final class MerlinDataExchangeParameters
{
    private final Instant _start;
    private final Instant _end;
    private final StoreOption _storeOption;
    private final String _fPartOverride;
    private final Path _watershedDirectory;
    private final Path _logFileDirectory;
    private final AuthenticationParameters[] _authenticationParameters;

    public MerlinDataExchangeParameters(Path watershedDirectory, Path logFileDirectory, Instant start, Instant end,
                                        StoreOption storeOption, String fPartOverride, AuthenticationParameters... authenticationParameters)
    {
        _authenticationParameters = authenticationParameters;
        _watershedDirectory = watershedDirectory;
        _logFileDirectory = logFileDirectory;
        _start = start;
        _end = end;
        _storeOption = storeOption;
        _fPartOverride = fPartOverride;
    }

    public Instant getStart()
    {
        return _start;
    }

    public Instant getEnd()
    {
        return _end;
    }

    public StoreOption getStoreOption()
    {
        return _storeOption;
    }

    public String getFPartOverride()
    {
        return _fPartOverride;
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
        return Arrays.stream(_authenticationParameters)
                .filter(authParam -> authParam.getUrl().equalsIgnoreCase(url))
                .findFirst()
                .orElseThrow(() -> new UsernamePasswordNotFoundException(url))
                .getUsernamePassword();
    }
}
