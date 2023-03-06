package gov.usbr.wq.merlindataexchange.parameters;

import gov.usbr.wq.merlindataexchange.fluentbuilders.FluentAlterParametersBuilder;
import gov.usbr.wq.merlindataexchange.fluentbuilders.FluentAuthenticationParameters;
import gov.usbr.wq.merlindataexchange.fluentbuilders.FluentFromExistingParameters;
import gov.usbr.wq.merlindataexchange.fluentbuilders.FluentLogDirectory;
import gov.usbr.wq.merlindataexchange.fluentbuilders.FluentParamatersBuilder;
import gov.usbr.wq.merlindataexchange.fluentbuilders.FluentParametersNonRequiredBuilder;
import gov.usbr.wq.merlindataexchange.fluentbuilders.FluentStoreOption;
import gov.usbr.wq.merlindataexchange.fluentbuilders.FluentWatershedDirectory;
import hec.io.StoreOption;

import java.nio.file.Path;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

public final class MerlinParametersBuilder implements FluentWatershedDirectory, FluentFromExistingParameters
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

    @Override
    public FluentAlterParametersBuilder fromExistingParameters(MerlinParameters existingParameters)
    {
        if(existingParameters == null)
        {
            throw new IllegalArgumentException("Missing required parameters to copy from");
        }
        _watershedDirectory = existingParameters.getWatershedDirectory();
        _logDirectory = existingParameters.getLogFileDirectory();
        _start = existingParameters.getStart();
        _end = existingParameters.getEnd();
        _storeOption = existingParameters.getStoreOption();
        _fPartOverride = existingParameters.getFPartOverride();
        _authenticationParametersList = existingParameters.getAuthenticationParameters();
        return new MerlinAlteredParameters();
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

    private class MerlinAlteredParameters extends MerlinParametersBuild implements FluentAlterParametersBuilder
    {

        @Override
        public FluentAlterParametersBuilder withWatershedDirectory(Path watershedDirectory)
        {
            _watershedDirectory = Objects.requireNonNull(watershedDirectory, "Watershed directory must be specified, not null");
            return this;
        }

        @Override
        public FluentAlterParametersBuilder withLogFileDirectory(Path logDirectory)
        {
            _logDirectory = Objects.requireNonNull(logDirectory, "Log file directory must be specified, not null");
            return this;
        }

        @Override
        public FluentAlterParametersBuilder withStoreOption(StoreOption storeOption)
        {
            _storeOption = Objects.requireNonNull(storeOption, "Store option must be specified, not null");
            return this;
        }

        @Override
        public FluentAlterParametersBuilder withAuthenticationParameters(AuthenticationParameters authenticationParameters)
        {
            if(authenticationParameters == null)
            {
                throw new IllegalArgumentException("AuthenticationParameters must be specified, not null");
            }
            return withAuthenticationParametersList(Collections.singletonList(authenticationParameters));
        }

        @Override
        public FluentAlterParametersBuilder withAuthenticationParametersList(List<AuthenticationParameters> authenticationParameters)
        {
            if(authenticationParameters == null)
            {
                throw new IllegalArgumentException("AuthenticationParameters must be specified, not null");
            }
            _authenticationParametersList = authenticationParameters;
            return this;
        }

        @Override
        public FluentAlterParametersBuilder withStart(Instant start)
        {
            _start = start;
            return this;
        }

        @Override
        public FluentAlterParametersBuilder withEnd(Instant end)
        {
            _end = end;
            return this;
        }

        @Override
        public FluentAlterParametersBuilder withFPartOverride(String fPartOverride)
        {
            _fPartOverride = fPartOverride;
            return this;
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

    private class MerlinParametersNonRequiredBuilder extends MerlinParametersBuild implements FluentParametersNonRequiredBuilder
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
    }

    private class MerlinParametersBuild implements FluentParamatersBuilder
    {
        @Override
        public MerlinParameters build()
        {
            return new MerlinParameters(_watershedDirectory, _logDirectory, _start, _end, _storeOption, _fPartOverride, _authenticationParametersList);
        }
    }
}
