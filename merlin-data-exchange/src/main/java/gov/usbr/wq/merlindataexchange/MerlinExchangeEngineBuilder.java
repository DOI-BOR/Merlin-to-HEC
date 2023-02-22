package gov.usbr.wq.merlindataexchange;

import gov.usbr.wq.merlindataexchange.fluentbuilders.FluentBuilder;
import gov.usbr.wq.merlindataexchange.fluentbuilders.FluentBuilderDataExchangeConfigurationFiles;
import gov.usbr.wq.merlindataexchange.fluentbuilders.FluentBuilderDataExchangeParameters;
import gov.usbr.wq.merlindataexchange.fluentbuilders.FluentBuilderProgressListener;
import gov.usbr.wq.merlindataexchange.parameters.MerlinParameters;
import hec.ui.ProgressListener;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

public final class MerlinExchangeEngineBuilder implements FluentBuilderDataExchangeConfigurationFiles
{
    private List<Path> _configurationFiles = new ArrayList<>();
    private MerlinParameters _runtimeParameters;
    private ProgressListener _progressListener;

    @Override
    public FluentBuilderDataExchangeParameters withConfigurationFiles(List<Path> configurationFiles)
    {
        _configurationFiles = configurationFiles;
        return new FluentMerlinDataExchangeParameters();
    }

    private class FluentMerlinDataExchangeParameters implements FluentBuilderDataExchangeParameters
    {

        @Override
        public FluentBuilderProgressListener withParameters(MerlinParameters runtimeParameters)
        {
            _runtimeParameters = runtimeParameters;
            return new FluentMerlinDataExchangeProgressListener();
        }
    }

    private class FluentMerlinDataExchangeProgressListener implements FluentBuilderProgressListener
    {
        @Override
        public FluentBuilder withProgressListener(ProgressListener progressListener)
        {
            _progressListener = progressListener;
            return new FluentMerlinBuilderImpl();
        }
    }

    private class FluentMerlinBuilderImpl implements FluentBuilder
    {
        @Override
        public DataExchangeEngine build()
        {
            return new MerlinDataExchangeEngine(_configurationFiles, _runtimeParameters, _progressListener);
        }
    }
}
