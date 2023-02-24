package gov.usbr.wq.merlindataexchange.fluentbuilders;

import gov.usbr.wq.merlindataexchange.parameters.AuthenticationParameters;

import java.util.List;

public interface FluentAuthenticationParameters
{
    FluentStoreOption withAuthenticationParameters(AuthenticationParameters authenticationParameters);
    FluentStoreOption withAuthenticationParametersList(List<AuthenticationParameters> authenticationParametersList);
}
