package gov.usbr.wq.merlindataexchange.parameters;

import gov.usbr.wq.merlindataexchange.fluentbuilders.parameters.timeseries.FluentTimeSeriesAlterParametersBuilder;
import gov.usbr.wq.merlindataexchange.fluentbuilders.parameters.timeseries.FluentTimeSeriesStoreOption;
import gov.usbr.wq.merlindataexchange.fluentbuilders.parameters.timeseries.FluentTimeSeriesAuthenticationParameters;
import gov.usbr.wq.merlindataexchange.fluentbuilders.parameters.timeseries.FluentTimeSeriesFromExistingParameters;
import gov.usbr.wq.merlindataexchange.fluentbuilders.parameters.timeseries.FluentTimeSeriesLogDirectory;
import gov.usbr.wq.merlindataexchange.fluentbuilders.parameters.timeseries.FluentTimeSeriesParametersBuilder;
import gov.usbr.wq.merlindataexchange.fluentbuilders.parameters.timeseries.FluentTimeSeriesParametersNonRequiredBuilder;
import gov.usbr.wq.merlindataexchange.fluentbuilders.parameters.timeseries.FluentTimeSeriesWatershedDirectory;
import hec.io.StoreOption;

import java.nio.file.Path;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

public final class MerlinTimeSeriesParametersBuilder implements FluentTimeSeriesWatershedDirectory, FluentTimeSeriesFromExistingParameters
{
    private Instant _start;
    private Instant _end;
    private Path _watershedDirectory;
    private Path _logDirectory;
    private List<AuthenticationParameters> _authenticationParametersList;
    private StoreOption _storeOption;
    private String _fPartOverride;
    @Override
    public FluentTimeSeriesLogDirectory withWatershedDirectory(Path watershedDirectory)
    {
        _watershedDirectory = Objects.requireNonNull(watershedDirectory, "Watershed directory must be specified, not null");
        return new MerlinTimeSeriesLogDirectory();
    }

    @Override
    public FluentTimeSeriesAlterParametersBuilder fromExistingParameters(MerlinParameters existingParameters)
    {
        if(existingParameters == null)
        {
            throw new IllegalArgumentException("Missing required parameters to copy from");
        }
        _watershedDirectory = existingParameters.getWatershedDirectory();
        _logDirectory = existingParameters.getLogFileDirectory();
        _start = existingParameters.getStart();
        _end = existingParameters.getEnd();
        _authenticationParametersList = existingParameters.getAuthenticationParameters();
        if(existingParameters instanceof MerlinTimeSeriesParameters)
        {
            _storeOption = ((MerlinTimeSeriesParameters)existingParameters).getStoreOption();
            _fPartOverride = ((MerlinTimeSeriesParameters)existingParameters).getFPartOverride();
        }
        return new MerlinTimeSeriesAlteredParameters();
    }

    private class MerlinTimeSeriesLogDirectory implements FluentTimeSeriesLogDirectory
    {

        @Override
        public FluentTimeSeriesAuthenticationParameters withLogFileDirectory(Path logDirectory)
        {
            _logDirectory = Objects.requireNonNull(logDirectory, "Log file directory must be specified, not null");
            return new MerlinTimeSeriesAuthenticationParameters();
        }
    }

    private class MerlinTimeSeriesAuthenticationParameters implements FluentTimeSeriesAuthenticationParameters
    {
        @Override
        public FluentTimeSeriesStoreOption withAuthenticationParameters(AuthenticationParameters authenticationParameters)
        {
            if(authenticationParameters == null)
            {
                throw new IllegalArgumentException("AuthenticationParameters must be specified, not null");
            }
            _authenticationParametersList = Collections.singletonList(authenticationParameters);
            return new MerlinStoreOption();
        }

        @Override
        public FluentTimeSeriesStoreOption withAuthenticationParametersList(List<AuthenticationParameters> authenticationParametersList)
        {
            if(authenticationParametersList == null || authenticationParametersList.isEmpty())
            {
                throw new IllegalArgumentException("AuthenticationParameters must be specified, not null");
            }
            _authenticationParametersList = authenticationParametersList;
            return new MerlinStoreOption();
        }
    }

    private class MerlinStoreOption implements FluentTimeSeriesStoreOption
    {

        @Override
        public FluentTimeSeriesParametersNonRequiredBuilder withStoreOption(StoreOption storeOption)
        {
            _storeOption = Objects.requireNonNull(storeOption, "Store option must be specified, not null");
            return new MerlinTimeSeriesParametersNonRequiredBuilder();
        }
    }

    private class MerlinTimeSeriesParametersNonRequiredBuilder extends MerlinTimeSeriesParametersBuild implements FluentTimeSeriesParametersNonRequiredBuilder
    {
        @Override
        public FluentTimeSeriesParametersNonRequiredBuilder withFPartOverride(String fPartOverride)
        {
            _fPartOverride = fPartOverride;
            return this;
        }

        @Override
        public FluentTimeSeriesParametersNonRequiredBuilder withStart(Instant start)
        {
            _start = start;
            return this;
        }

        @Override
        public FluentTimeSeriesParametersNonRequiredBuilder withEnd(Instant end)
        {
            _end = end;
            return this;
        }

    }

    private class MerlinTimeSeriesParametersBuild implements FluentTimeSeriesParametersBuilder
    {
        @Override
        public MerlinTimeSeriesParameters build()
        {
            return new MerlinTimeSeriesParameters(_watershedDirectory, _logDirectory, _start, _end, _storeOption, _fPartOverride, _authenticationParametersList);
        }
    }

    private class MerlinTimeSeriesAlteredParameters extends MerlinTimeSeriesParametersBuild implements FluentTimeSeriesAlterParametersBuilder
    {

        @Override
        public FluentTimeSeriesAlterParametersBuilder withWatershedDirectory(Path watershedDirectory)
        {
            _watershedDirectory = Objects.requireNonNull(watershedDirectory, "Watershed directory must be specified, not null");
            return this;
        }

        @Override
        public FluentTimeSeriesAlterParametersBuilder withLogFileDirectory(Path logDirectory)
        {
            _logDirectory = Objects.requireNonNull(logDirectory, "Log file directory must be specified, not null");
            return this;
        }

        @Override
        public FluentTimeSeriesAlterParametersBuilder withAuthenticationParameters(AuthenticationParameters authenticationParameters)
        {
            if(authenticationParameters == null)
            {
                throw new IllegalArgumentException("AuthenticationParameters must be specified, not null");
            }
            return withAuthenticationParametersList(Collections.singletonList(authenticationParameters));
        }

        @Override
        public FluentTimeSeriesAlterParametersBuilder withAuthenticationParametersList(List<AuthenticationParameters> authenticationParameters)
        {
            if(authenticationParameters == null)
            {
                throw new IllegalArgumentException("AuthenticationParameters must be specified, not null");
            }
            _authenticationParametersList = authenticationParameters;
            return this;
        }

        @Override
        public FluentTimeSeriesAlterParametersBuilder withStart(Instant start)
        {
            _start = start;
            return this;
        }

        @Override
        public FluentTimeSeriesAlterParametersBuilder withEnd(Instant end)
        {
            _end = end;
            return this;
        }

        @Override
        public FluentTimeSeriesAlterParametersBuilder withStoreOption(StoreOption storeOption)
        {
            _storeOption = Objects.requireNonNull(storeOption, "Store option must be specified, not null");
            return this;
        }

        @Override
        public FluentTimeSeriesAlterParametersBuilder withFPartOverride(String fPartOverride)
        {
            _fPartOverride = fPartOverride;
            return this;
        }
    }
}
