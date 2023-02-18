package gov.usbr.wq.merlindataexchange.parameters;

import hec.io.StoreOption;

import java.nio.file.Path;
import java.time.Instant;
import java.util.Collections;
import java.util.List;

public final class MerlinParametersBuilder
{
    private Instant _start;
    private Instant _end;
    private StoreOption _storeOption;
    private String _fPartOverride;
    private Path _watershedDirectory;
    private Path _logFileDirectory;
    private List<AuthenticationParameters> _authenticationParameters;

    public MerlinParametersBuilder withStart(Instant start)
    {
        _start = start;
        return this;
    }

    public MerlinParametersBuilder withEnd(Instant end)
    {
        _end = end;
        return this;
    }

    public MerlinParametersBuilder withStoreOption(StoreOption storeOption)
    {
        _storeOption = storeOption;
        return this;
    }

    public MerlinParametersBuilder withFPartOverride(String fPartOverride)
    {
        _fPartOverride = fPartOverride;
        return this;
    }

    public MerlinParametersBuilder withWatershedDirectory(Path watershedDirectory)
    {
        _watershedDirectory = watershedDirectory;
        return this;
    }

    public MerlinParametersBuilder withLogFileDirectory(Path logFileDirectory)
    {
        _logFileDirectory = logFileDirectory;
        return this;
    }

    public MerlinParametersBuilder withAuthenticationParametersList(List<AuthenticationParameters> authenticationParameters)
    {
        _authenticationParameters = authenticationParameters;
        return this;
    }

    public MerlinParametersBuilder withAuthenticationParameters(AuthenticationParameters authenticationParameters)
    {
        _authenticationParameters = Collections.singletonList(authenticationParameters);
        return this;
    }

    public MerlinParameters build()
    {
        // create and return a new MerlinParameters object with the builder's current state
        return new MerlinParameters(_watershedDirectory, _logFileDirectory, _start, _end, _storeOption, _fPartOverride, _authenticationParameters);
    }
}
