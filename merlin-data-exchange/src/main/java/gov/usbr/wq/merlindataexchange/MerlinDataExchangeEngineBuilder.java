package gov.usbr.wq.merlindataexchange;

import gov.usbr.wq.merlindataexchange.fluentbuilders.FluentEngineBuilder;
import gov.usbr.wq.merlindataexchange.fluentbuilders.FluentBuilderDataExchangeConfigurationFiles;
import gov.usbr.wq.merlindataexchange.fluentbuilders.FluentBuilderDataExchangeParameters;
import gov.usbr.wq.merlindataexchange.fluentbuilders.FluentBuilderProgressListener;
import gov.usbr.wq.merlindataexchange.parameters.MerlinParameters;
import hec.ui.ProgressListener;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public final class MerlinDataExchangeEngineBuilder implements FluentBuilderDataExchangeConfigurationFiles
{
    private List<Path> _configurationFiles = new ArrayList<>();
    private MerlinParameters _runtimeParameters;
    private ProgressListener _progressListener;

    @Override
    public FluentBuilderDataExchangeParameters withConfigurationFiles(List<Path> configurationFiles)
    {
        if(configurationFiles == null || configurationFiles.isEmpty())
        {
            throw new IllegalArgumentException("At least 1 configuration file must be specified");
        }
        _configurationFiles = configurationFiles;
        return new FluentMerlinDataExchangeParameters();
    }

    private class FluentMerlinDataExchangeParameters implements FluentBuilderDataExchangeParameters
    {

        @Override
        public FluentBuilderProgressListener withParameters(MerlinParameters runtimeParameters)
        {
            _runtimeParameters = Objects.requireNonNull(runtimeParameters, "Parameters must be specified, not null");
            return new FluentMerlinDataExchangeProgressListener();
        }
    }

    private class FluentMerlinDataExchangeProgressListener implements FluentBuilderProgressListener
    {
        @Override
        public FluentEngineBuilder withProgressListener(ProgressListener progressListener)
        {
            _progressListener = Objects.requireNonNull(progressListener, "Progress listener must be specified, not null");
            return new FluentMerlinBuilderImpl();
        }
    }

    private class FluentMerlinBuilderImpl implements FluentEngineBuilder
    {
        @Override
        public DataExchangeEngine build()
        {
            return new MerlinDataExchangeEngine(_configurationFiles, _runtimeParameters, _progressListener);
        }
    }
}
