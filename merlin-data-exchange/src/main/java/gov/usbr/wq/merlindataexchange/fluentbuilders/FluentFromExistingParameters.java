package gov.usbr.wq.merlindataexchange.fluentbuilders;

import gov.usbr.wq.merlindataexchange.parameters.MerlinParameters;

public interface FluentFromExistingParameters
{
    FluentAlterParametersBuilder fromExistingParameters(MerlinParameters existingParameters);
}
