package gov.usbr.wq.merlindataexchange.fluentbuilders;

import java.nio.file.Path;

public interface FluentLogDirectory
{
    FluentAuthenticationParameters withLogFileDirectory(Path logDirectory);
}
