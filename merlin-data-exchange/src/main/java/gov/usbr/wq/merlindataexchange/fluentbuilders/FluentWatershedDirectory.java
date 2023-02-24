package gov.usbr.wq.merlindataexchange.fluentbuilders;

import java.nio.file.Path;

public interface FluentWatershedDirectory
{
    FluentLogDirectory withWatershedDirectory(Path watershedDirectory);
}
