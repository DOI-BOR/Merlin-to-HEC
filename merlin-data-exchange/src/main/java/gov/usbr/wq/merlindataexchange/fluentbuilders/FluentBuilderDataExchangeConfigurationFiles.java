package gov.usbr.wq.merlindataexchange.fluentbuilders;

import java.nio.file.Path;
import java.util.List;

public interface FluentBuilderDataExchangeConfigurationFiles
{
    FluentBuilderDataExchangeParameters withConfigurationFiles(List<Path> configurationFiles);
}
