package gov.usbr.wq.merlindataexchange;

import gov.usbr.wq.dataaccess.http.ApiConnectionInfo;
import gov.usbr.wq.dataaccess.http.HttpAccessException;
import gov.usbr.wq.dataaccess.http.HttpAccessUtils;
import gov.usbr.wq.dataaccess.http.TokenContainer;
import gov.usbr.wq.dataaccess.model.MeasureWrapper;
import gov.usbr.wq.dataaccess.model.TemplateWrapper;
import gov.usbr.wq.merlindataexchange.fluentbuilders.ExportType;
import gov.usbr.wq.merlindataexchange.io.TemplateMeasureCsvWriter;
import gov.usbr.wq.merlindataexchange.io.TemplateMeasureReader;
import gov.usbr.wq.merlindataexchange.io.TemplateMeasureXlsxWriter;
import gov.usbr.wq.merlindataexchange.parameters.AuthenticationParameters;
import gov.usbr.wq.merlindataexchange.parameters.UsernamePasswordHolder;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.logging.Level;
import java.util.logging.Logger;

public class MerlinDataExportEngine extends MerlinEngine implements DataExportEngine
{
    private final Logger LOGGER = Logger.getLogger(MerlinDataExportEngine.class.getName());
    private final AuthenticationParameters _authenticationParameters;
    private final String _exportFilePath;
    private final ExportType _exportType;

    public MerlinDataExportEngine(AuthenticationParameters authenticationParameters, String exportFilePath, ExportType exportType)
    {
        _authenticationParameters = authenticationParameters;
        _exportFilePath = exportFilePath;
        _exportType = exportType;
    }

    @Override
    public CompletableFuture<MerlinDataExchangeStatus> runExport()
    {
        switch (_exportType)
        {
            case JSON:
                throw new IllegalArgumentException("Unsupported export type: " + _exportType);
            case XLSX:
            case CSV:
                return runExport(_exportType);
            default:
                throw new IllegalArgumentException("Unsupported export type: " + _exportType);
        }
    }

    private CompletableFuture<MerlinDataExchangeStatus> runExport(ExportType exportType)
    {
        return CompletableFuture.supplyAsync(() ->
        {
            try
            {
                TokenContainer token = getToken(_authenticationParameters.getUrl(), _authenticationParameters.getUsernamePassword());
                ApiConnectionInfo connectionInfo = new ApiConnectionInfo(_authenticationParameters.getUrl());
                Map<TemplateWrapper, List<MeasureWrapper>> templateMeasureMap = new TemplateMeasureReader().collectTemplateMeasureData(token, connectionInfo, getExecutorService());
                switch (exportType)
                {
                    case CSV:
                        new TemplateMeasureCsvWriter().write(templateMeasureMap, _exportFilePath);
                        break;
                    case XLSX:
                        new TemplateMeasureXlsxWriter().write(templateMeasureMap, _exportFilePath);
                        break;
                    default:
                        throw new IllegalArgumentException("Unsupported export type: " + exportType);
                }
                return MerlinDataExchangeStatus.COMPLETE_SUCCESS;
            } catch (Exception e)
            {
                LOGGER.log(Level.SEVERE, "Error exporting data", e);
                return MerlinDataExchangeStatus.FAILURE;
            }
        });

    }

    public TokenContainer getToken(String url, UsernamePasswordHolder usernamePassword) throws MerlinAuthorizationException
    {
        TokenContainer token;
        ApiConnectionInfo connectionInfo = new ApiConnectionInfo(url);
        try
        {
            token = HttpAccessUtils.authenticate(connectionInfo, usernamePassword.getUsername(), usernamePassword.getPassword());
        } catch (HttpAccessException e)
        {
            throw new MerlinAuthorizationException(e, usernamePassword, connectionInfo);
        }
        return token;
    }

}
