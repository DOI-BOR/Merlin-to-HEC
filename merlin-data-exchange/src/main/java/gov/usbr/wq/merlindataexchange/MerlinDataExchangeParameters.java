package gov.usbr.wq.merlindataexchange;

import hec.io.StoreOption;

import java.nio.file.Path;
import java.time.Instant;
import java.util.Arrays;

public final class MerlinDataExchangeParameters
{
    private final String _username;
    private final char[] _password;
    private final Instant _start;
    private final Instant _end;
    private final StoreOption _storeOption;
    private final String _fPartOverride;
    private final Path _watershedDirectory;

    public MerlinDataExchangeParameters(String username, char[] password, Path watershedDirectory, Instant start, Instant end,
                                        StoreOption storeOption, String fPartOverride)
    {
        _username = username;
        _password = password;
        _watershedDirectory = watershedDirectory;
        _start = start;
        _end = end;
        _storeOption = storeOption;
        _fPartOverride = fPartOverride;
    }

    String getUsername()
    {
        return _username;
    }

    char[] getPassword()
    {
        return _password;
    }

    Instant getStart()
    {
        return _start;
    }

    Instant getEnd()
    {
        return _end;
    }

    StoreOption getStoreOption()
    {
        return _storeOption;
    }

    String getFPartOverride()
    {
        return _fPartOverride;
    }

    Path getWatershedDirectory()
    {
        return _watershedDirectory;
    }

    void clearPassword()
    {
        Arrays.fill(_password, '\0');
    }
}
