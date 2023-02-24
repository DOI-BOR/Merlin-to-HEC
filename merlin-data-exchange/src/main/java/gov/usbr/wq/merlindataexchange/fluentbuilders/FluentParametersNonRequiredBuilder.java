package gov.usbr.wq.merlindataexchange.fluentbuilders;

import gov.usbr.wq.merlindataexchange.parameters.MerlinParameters;

import java.time.Instant;

public interface FluentParametersNonRequiredBuilder
{
    FluentParametersNonRequiredBuilder withFPartOverride(String fPartOverride);
    FluentParametersNonRequiredBuilder withStart(Instant start);

    FluentParametersNonRequiredBuilder withEnd(Instant end);

    MerlinParameters build();

}
