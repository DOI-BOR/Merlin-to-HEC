package gov.usbr.wq.merlindataexchange.fluentbuilders;

import hec.ui.ProgressListener;

public interface FluentBuilderProgressListener
{
    FluentEngineBuilder withProgressListener(ProgressListener progressListener);
}
