package gov.usbr.wq.merlindataexchange.io.wq;

import java.util.LinkedHashMap;
import java.util.Map;

final class CsvDataMapping
{
    private final Map<String, Double> _parameterValues = new LinkedHashMap<>();

    Map<String, Double> getParameterValues()
    {
        return _parameterValues;
    }

    void setParameterValue(String parameterHeader, Double value)
    {
        _parameterValues.put(parameterHeader, value);
    }
}
