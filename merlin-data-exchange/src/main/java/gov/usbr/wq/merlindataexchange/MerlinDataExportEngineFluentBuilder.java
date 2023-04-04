package gov.usbr.wq.merlindataexchange;

import gov.usbr.wq.merlindataexchange.fluentbuilders.ExportType;
import gov.usbr.wq.merlindataexchange.fluentbuilders.FluentDataExportBuilder;
import gov.usbr.wq.merlindataexchange.fluentbuilders.FluentExportFile;
import gov.usbr.wq.merlindataexchange.fluentbuilders.FluentExportType;
import gov.usbr.wq.merlindataexchange.parameters.AuthenticationParameters;

public final class MerlinDataExportEngineFluentBuilder {
    private AuthenticationParameters _authenticationParameters;
    private String _exportFilePath;
    private ExportType _exportType;

    public FluentExportFile withAuthenticationParameters(AuthenticationParameters authenticationParameters) {
        _authenticationParameters = authenticationParameters;
        return new MerlinExportFileBuilder();
    }

    private class MerlinExportFileBuilder implements FluentExportFile {
        @Override
        public FluentExportType withExportFilePath(String exportFilePath) {
            _exportFilePath = exportFilePath;
            return new MerlinExportTypeBuilder();
        }

    }

    private class MerlinExportTypeBuilder implements FluentExportType {
        @Override
        public FluentDataExportBuilder withExportType(ExportType exportType) {
            _exportType = exportType;
            return new MerlinDataExportBuilder();
        }
    }

    private class MerlinDataExportBuilder implements FluentDataExportBuilder {
        @Override
        public DataExportEngine build() {
            return new MerlinDataExportEngine(_authenticationParameters, _exportFilePath, _exportType);
        }
    }
}
