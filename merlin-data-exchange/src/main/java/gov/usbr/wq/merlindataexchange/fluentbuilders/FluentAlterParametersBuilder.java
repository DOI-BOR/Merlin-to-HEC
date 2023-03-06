package gov.usbr.wq.merlindataexchange.fluentbuilders;

import gov.usbr.wq.merlindataexchange.parameters.AuthenticationParameters;
import hec.io.StoreOption;

import java.nio.file.Path;
import java.time.Instant;
import java.util.List;

public interface FluentAlterParametersBuilder extends FluentParamatersBuilder
{
    FluentAlterParametersBuilder withWatershedDirectory(Path watershedDirectory);
    FluentAlterParametersBuilder withLogFileDirectory(Path logDirectory);
    FluentAlterParametersBuilder withStoreOption(StoreOption storeOption);
    FluentAlterParametersBuilder withAuthenticationParameters(AuthenticationParameters parameters);
    FluentAlterParametersBuilder withAuthenticationParametersList(List<AuthenticationParameters> parameters);
    FluentAlterParametersBuilder withStart(Instant start);
    FluentAlterParametersBuilder withEnd(Instant end);
    FluentAlterParametersBuilder withFPartOverride(String fPartOverride);


}
