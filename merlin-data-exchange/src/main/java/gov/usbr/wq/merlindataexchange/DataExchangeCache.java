package gov.usbr.wq.merlindataexchange;

import gov.usbr.wq.dataaccess.model.MeasureWrapper;
import gov.usbr.wq.dataaccess.model.QualityVersionWrapper;
import gov.usbr.wq.dataaccess.model.TemplateWrapper;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public final class DataExchangeCache
{
    private final Set<TemplateWrapper> _cachedTemplates = new HashSet<>();
    private final Set<QualityVersionWrapper> _cachedQualityVersions = new HashSet<>();
    private final Map<TemplateWrapper, List<MeasureWrapper>> _cachedTemplateToMeasures = new HashMap<>();

    public List<TemplateWrapper> getCachedTemplates()
    {
        return new ArrayList<>(_cachedTemplates);
    }

    public List<QualityVersionWrapper> getCachedQualityVersions()
    {
        return new ArrayList<>(_cachedQualityVersions);
    }

    public Map<TemplateWrapper, List<MeasureWrapper>> getCachedTemplateToMeasures()
    {
        return new HashMap<>(_cachedTemplateToMeasures);
    }

    public void cacheTemplates(List<TemplateWrapper> templates)
    {
        _cachedTemplates.addAll(templates);
    }

    public void cacheQualityVersions(List<QualityVersionWrapper> qualityVersions)
    {
        _cachedQualityVersions.addAll(qualityVersions);
    }
    public void cacheMeasures(TemplateWrapper template, List<MeasureWrapper> measures)
    {
        _cachedTemplateToMeasures.put(template, measures);
    }

    public void clearCache()
    {
        _cachedTemplates.clear();
        _cachedQualityVersions.clear();
        _cachedTemplateToMeasures.clear();
    }

}
