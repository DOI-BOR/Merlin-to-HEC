package gov.usbr.wq.merlindataexchange.io.wq;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Used for serializing a mapping of headers to data values for a given CSV row
 */
final class CsvDataMapping
{
    private final Map<String, Double> _headerToValuesMap = new LinkedHashMap<>();

    Map<String, Double> getHeaderToValuesMap()
    {
        return _headerToValuesMap;
    }
}
