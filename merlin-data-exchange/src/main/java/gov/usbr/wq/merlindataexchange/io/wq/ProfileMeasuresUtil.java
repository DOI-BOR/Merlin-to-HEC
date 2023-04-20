package gov.usbr.wq.merlindataexchange.io.wq;

import gov.usbr.wq.dataaccess.model.MeasureWrapper;
import gov.usbr.wq.dataaccess.model.TemplateWrapper;
import gov.usbr.wq.merlindataexchange.DataExchangeCache;
import gov.usbr.wq.merlindataexchange.configuration.DataExchangeSet;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.regex.Pattern;

import static java.util.stream.Collectors.toList;

final class ProfileMeasuresUtil
{
    private ProfileMeasuresUtil()
    {
        throw new AssertionError("Utility class");
    }
    static List<MeasureWrapper> getMeasuresListForDepthMeasure(MeasureWrapper depthMeasure, DataExchangeSet dataExchangeSet, DataExchangeCache cache)
    {
        List<MeasureWrapper> retVal = new ArrayList<>();
        String[] split = depthMeasure.getSeriesString().split("/");
        String regex = "^" + split[0] +"/[^/]+/" + split[2] + "/" + split[3] + "/" + split[4] + "/" + split[5].substring(0, split[5].length()-1) + "\\d+$";
        Pattern pattern = Pattern.compile(regex);
        Optional<TemplateWrapper> template = cache.getCachedTemplates().stream()
                .filter(t -> t.getName().equalsIgnoreCase(dataExchangeSet.getTemplateName())
                        || Objects.equals(t.getDprId(), dataExchangeSet.getTemplateId()))
                .findFirst();
        if(template.isPresent())
        {
            List<MeasureWrapper> measures = cache.getCachedTemplateToMeasures().get(template.get());
            retVal = measures.stream().filter(m -> pattern.matcher(m.getSeriesString()).matches()
                            && m.getType().equalsIgnoreCase(MerlinDataExchangeProfileReader.PROFILE))
                    .collect(toList());
        }
        return retVal;
    }

    static Double getMinDepth(List<Double> depthValues)
    {
        return depthValues.stream()
                .mapToDouble(Double::doubleValue)
                .min()
                .orElse(Double.MIN_VALUE);
    }

    static Double getMaxDepth(List<Double> depthValues)
    {
        return depthValues.stream()
                .mapToDouble(Double::doubleValue)
                .max()
                .orElse(Double.MAX_VALUE);
    }
}
