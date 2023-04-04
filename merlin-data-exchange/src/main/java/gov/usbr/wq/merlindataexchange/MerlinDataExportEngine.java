package gov.usbr.wq.merlindataexchange;

import java.io.IOException;
import java.text.MessageFormat;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import gov.usbr.wq.dataaccess.MerlinTimeSeriesDataAccess;
import gov.usbr.wq.dataaccess.http.ApiConnectionInfo;
import gov.usbr.wq.dataaccess.http.HttpAccessException;
import gov.usbr.wq.dataaccess.http.HttpAccessUtils;
import gov.usbr.wq.dataaccess.http.TokenContainer;
import gov.usbr.wq.dataaccess.model.MeasureWrapper;
import gov.usbr.wq.dataaccess.model.TemplateWrapper;
import gov.usbr.wq.merlindataexchange.fluentbuilders.ExportType;
import gov.usbr.wq.merlindataexchange.parameters.AuthenticationParameters;
import gov.usbr.wq.merlindataexchange.parameters.UsernamePasswordHolder;

public class MerlinDataExportEngine extends MerlinEngine implements DataExportEngine {
    private final Logger LOGGER = Logger.getLogger(MerlinDataExportEngine.class.getName());

    private final AuthenticationParameters _authenticationParameters;
    private final String _exportFilePath;
    private final ExportType _exportType;

    public MerlinDataExportEngine(AuthenticationParameters authenticationParameters, String exportFilePath, ExportType exportType) {
        _authenticationParameters = authenticationParameters;
        _exportFilePath = exportFilePath;
        _exportType = exportType;
    }

    @Override
    public CompletableFuture<MerlinDataExchangeStatus> runExport() {
        switch (_exportType) {
            case JSON:
                throw new IllegalArgumentException("Unsupported export type: " + _exportType);
            case CSV:
            default:
                return runCsvExport();
        }
    }

    private CompletableFuture<MerlinDataExchangeStatus> runCsvExport() {
        return CompletableFuture.supplyAsync(() -> {
            try {
                Map<TemplateWrapper, List<MeasureWrapper>> templateMeasureMap = collectTemplateMeasureData();
                writeToCsv(templateMeasureMap, _exportFilePath);
                return MerlinDataExchangeStatus.COMPLETE_SUCCESS;
            } catch (Exception e) {
                LOGGER.log(Level.SEVERE, "Error exporting data", e);
                return MerlinDataExchangeStatus.FAILURE;
            }
        });

    }

    private void writeToCsv(Map<TemplateWrapper, List<MeasureWrapper>> templateMeasureMap, String exportFilePath) {
        createCsvSchema();

    }

    private void createCsvSchema() {
        //create csv schema

        CsvSchema.Builder builder = CsvSchema.builder();

    }

    private Map<TemplateWrapper, List<MeasureWrapper>> collectTemplateMeasureData() throws MerlinAuthorizationException, IOException, HttpAccessException {
        //block until we are done
        //get token
        TokenContainer token = getToken(_authenticationParameters.getUrl(), _authenticationParameters.getUsernamePassword());

        //retrieve templates
        ApiConnectionInfo connectionInfo = new ApiConnectionInfo(_authenticationParameters.getUrl());
        List<TemplateWrapper> templates = getTemplates(connectionInfo, token);

        //        List<CompletableFuture<Map<TemplateWrapper, List<MeasureWrapper>>>> collect =
        TreeMap<TemplateWrapper, List<MeasureWrapper>> templateMeasureMap = templates.stream().map(template -> {
                    //retrieve measures for template
                    return CompletableFuture.supplyAsync(() -> {
                                try {
                                    Map<TemplateWrapper, List<MeasureWrapper>> innerTemplateMeasureMap = new TreeMap<>(Comparator.comparing(TemplateWrapper::getDprId));
                                    innerTemplateMeasureMap.put(template, getMeasuresForTemplate(connectionInfo, token, template));
                                    return innerTemplateMeasureMap;
                                } catch (IOException | HttpAccessException e) {
                                    throw new RuntimeException(e);
                                }
                            }, getExecutorService())
                            .handle((map, ex) -> {
                                if (ex != null) {
                                    LOGGER.log(Level.SEVERE, ex, () -> MessageFormat.format("Error retrieving measures for template:{0};{1} ", new Object[]{template.getDprId(), template.getName()}));
                                    map.put(template, Collections.emptyList());
                                }
                                return map;
                            });
                }).map(CompletableFuture::join)
                .flatMap(map -> map.entrySet().stream()) // Flatten Map<TemplateWrapper, List<MeasureWrapper>> to Stream<Entry<TemplateWrapper, List<MeasureWrapper>>>
                .collect(Collectors.toMap(
                        Map.Entry::getKey, // Key mapper
                        Map.Entry::getValue, // Value mapper
                        (list1, list2) -> {
                            list1.addAll(list2); // Merge lists if key already exists
                            return list1;
                        }
                        , () -> new TreeMap<>(Comparator.comparing(TemplateWrapper::getDprId))
                ));
        return templateMeasureMap;
    }

    private TemplateMeasureCsv buildCsvRow(final TemplateWrapper template, final MeasureWrapper measure) {
        TemplateMeasureCsv retval = new TemplateMeasureCsv();
        //        retval.setDprId(template.getDprId());
        //        retval.setTemplateName(template.getName());
        return retval;
    }

    private List<MeasureWrapper> getMeasuresForTemplate(final ApiConnectionInfo connectionInfo, final TokenContainer token, final TemplateWrapper template) throws IOException, HttpAccessException {
        return new MerlinTimeSeriesDataAccess().getMeasurementsByTemplate(connectionInfo, token, template);
    }

    private List<TemplateWrapper> getTemplates(ApiConnectionInfo connectionInfo, TokenContainer token) throws IOException, HttpAccessException {
        List<TemplateWrapper> templates = new MerlinTimeSeriesDataAccess().getTemplates(connectionInfo, token);
        return templates;
    }

    private TokenContainer getToken(String url, UsernamePasswordHolder usernamePassword) throws MerlinAuthorizationException {
        TokenContainer token;
        ApiConnectionInfo connectionInfo = new ApiConnectionInfo(url);
        try {
            token = HttpAccessUtils.authenticate(connectionInfo, usernamePassword.getUsername(), usernamePassword.getPassword());
        } catch (HttpAccessException e) {
            throw new MerlinAuthorizationException(e, usernamePassword, connectionInfo);
        }
        return token;
    }

}
