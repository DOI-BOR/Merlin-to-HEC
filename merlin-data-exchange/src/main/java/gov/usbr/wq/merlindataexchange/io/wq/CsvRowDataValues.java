package gov.usbr.wq.merlindataexchange.io.wq;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

import java.util.ArrayList;
import java.util.List;

@JsonPropertyOrder({ "Values" })
@JsonInclude(JsonInclude.Include.NON_NULL)
final class CsvRowDataValues
{
    @JsonProperty("Values")
    private List<Double> _values = new ArrayList<>();

    List<Double> getValues()
    {
        return _values;
    }

    void setValues(List<Double> values)
    {
        _values = values;
    }

    void addValue(double value)
    {
        _values.add(value);
    }
}
