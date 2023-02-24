package gov.usbr.wq.merlindataexchange.parameters;

import gov.usbr.wq.merlindataexchange.fluentbuilders.FluentAuthenticationParameters;
import gov.usbr.wq.merlindataexchange.fluentbuilders.FluentLogDirectory;
import gov.usbr.wq.merlindataexchange.fluentbuilders.FluentParametersNonRequiredBuilder;
import gov.usbr.wq.merlindataexchange.fluentbuilders.FluentStoreOption;
import gov.usbr.wq.merlindataexchange.fluentbuilders.FluentWatershedDirectory;
import hec.io.StoreOption;

import java.nio.file.Path;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

public final class MerlinParametersBuilder implements FluentWatershedDirectory
{
    private Path _logDirectory;
    private List<AuthenticationParameters> _authenticationParametersList;
    private Path _watershedDirectory;
    private StoreOption _storeOption;
    private String _fPartOverride;
    private Instant _start;
    private Instant _end;

    @Override
    public FluentLogDirectory withWatershedDirectory(Path watershedDirectory)
    {
        _watershedDirectory = Objects.requireNonNull(watershedDirectory, "Watershed directory must be specified, not null");
        return new MerlinLogDirectory();
    }

    private class MerlinLogDirectory implements FluentLogDirectory
    {

        @Override
        public FluentAuthenticationParameters withLogFileDirectory(Path logDirectory)
        {
            _logDirectory = Objects.requireNonNull(logDirectory, "Log file directory must be specified, not null");
            return new MerlinAuthenticationParameters();
        }
    }

    private class MerlinAuthenticationParameters implements FluentAuthenticationParameters
    {

        @Override
        public FluentStoreOption withAuthenticationParameters(AuthenticationParameters authenticationParameters)
        {
            if(authenticationParameters == null)
            {
                throw new IllegalArgumentException("AuthenticationParameters must be specified, not null");
            }
            _authenticationParametersList = Collections.singletonList(authenticationParameters);
            return new MerlinStoreOption();
        }

        @Override
        public FluentStoreOption withAuthenticationParametersList(List<AuthenticationParameters> authenticationParametersList)
        {
            if(authenticationParametersList == null || authenticationParametersList.isEmpty())
            {
                throw new IllegalArgumentException("AuthenticationParameters must be specified, not null");
            }
            _authenticationParametersList = authenticationParametersList;
            return new MerlinStoreOption();
        }
    }

    private class MerlinStoreOption implements FluentStoreOption
    {

        @Override
        public FluentParametersNonRequiredBuilder withStoreOption(StoreOption storeOption)
        {
            _storeOption = Objects.requireNonNull(storeOption, "Store option must be specified, not null");
            return new MerlinParametersNonRequiredBuilder();
        }
    }

    private class MerlinParametersNonRequiredBuilder implements FluentParametersNonRequiredBuilder
    {

        @Override
        public FluentParametersNonRequiredBuilder withFPartOverride(String fPartOverride)
        {
            _fPartOverride = fPartOverride;
            return this;
        }

        @Override
        public FluentParametersNonRequiredBuilder withStart(Instant start)
        {
            _start = start;
            return this;
        }

        @Override
        public FluentParametersNonRequiredBuilder withEnd(Instant end)
        {
            _end = end;
            return this;
        }

        @Override
        public MerlinParameters build()
        {
            return new MerlinParameters(_watershedDirectory, _logDirectory, _start, _end, _storeOption, _fPartOverride, _authenticationParametersList);
        }
    }
}
