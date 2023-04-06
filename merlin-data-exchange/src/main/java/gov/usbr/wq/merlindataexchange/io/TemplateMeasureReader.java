package gov.usbr.wq.merlindataexchange.io;

import gov.usbr.wq.dataaccess.MerlinTimeSeriesDataAccess;
import gov.usbr.wq.dataaccess.http.ApiConnectionInfo;
import gov.usbr.wq.dataaccess.http.HttpAccessException;
import gov.usbr.wq.dataaccess.http.HttpAccessUtils;
import gov.usbr.wq.dataaccess.http.TokenContainer;
import gov.usbr.wq.dataaccess.model.MeasureWrapper;
import gov.usbr.wq.dataaccess.model.TemplateWrapper;
import gov.usbr.wq.merlindataexchange.MerlinAuthorizationException;
import gov.usbr.wq.merlindataexchange.MerlinDataExportEngine;
import gov.usbr.wq.merlindataexchange.parameters.AuthenticationParameters;
import gov.usbr.wq.merlindataexchange.parameters.UsernamePasswordHolder;

import java.io.IOException;
import java.text.MessageFormat;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

public class TemplateMeasureReader
{
    private static final Logger LOGGER = Logger.getLogger(TemplateMeasureReader.class.getName());

    public Map<TemplateWrapper, List<MeasureWrapper>> collectTemplateMeasureData(TokenContainer token, ApiConnectionInfo connectionInfo, ExecutorService executorService) throws MerlinAuthorizationException, IOException, HttpAccessException
    {
        //block until we are done
        List<TemplateWrapper> templates = getTemplates(connectionInfo, token);

        //        List<CompletableFuture<Map<TemplateWrapper, List<MeasureWrapper>>>> collect =
        TreeMap<TemplateWrapper, List<MeasureWrapper>> templateMeasureMap = templates.stream().map(template ->
                {
                    //retrieve measures for template
                    return CompletableFuture.supplyAsync(() ->
                            {
                                try
                                {
                                    Map<TemplateWrapper, List<MeasureWrapper>> innerTemplateMeasureMap = new TreeMap<TemplateWrapper, List<MeasureWrapper>>(Comparator.comparing(TemplateWrapper::getDprId));
                                    innerTemplateMeasureMap.put(template, getMeasuresForTemplate(connectionInfo, token, template));
                                    return innerTemplateMeasureMap;
                                } catch (IOException | HttpAccessException e)
                                {
                                    throw new RuntimeException(e);
                                }
                            }, executorService)
                            .handle((map, ex) ->
                            {
                                if (ex != null)
                                {
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
                        (list1, list2) ->
                        {
                            list1.addAll(list2); // Merge lists if key already exists
                            return list1;
                        }
                        , () -> new TreeMap<TemplateWrapper, List<MeasureWrapper>>(Comparator.comparing(TemplateWrapper::getDprId))
                ));
        return templateMeasureMap;
    }

    public List<MeasureWrapper> getMeasuresForTemplate(final ApiConnectionInfo connectionInfo, final TokenContainer token, final TemplateWrapper template) throws IOException, HttpAccessException
    {
        return new MerlinTimeSeriesDataAccess().getMeasurementsByTemplate(connectionInfo, token, template);
    }

    public List<TemplateWrapper> getTemplates(ApiConnectionInfo connectionInfo, TokenContainer token) throws IOException, HttpAccessException
    {
        List<TemplateWrapper> templates = new MerlinTimeSeriesDataAccess().getTemplates(connectionInfo, token);
        return templates;
    }

}
