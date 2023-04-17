package gov.usbr.wq.merlindataexchange.io.wq;

import java.time.ZonedDateTime;

/**
 * Represents a single csv row, which contains a date-time, along with n number of constituent data values.
 */
final class CsvProfileRow
{
    private ZonedDateTime _date;
    private final CsvDataMapping _mapping = new CsvDataMapping();

    public ZonedDateTime getDate()
    {
        return _date;
    }

    public void setDate(ZonedDateTime date)
    {
        _date = date;
    }

    public CsvDataMapping getMapping()
    {
        return _mapping;
    }

    public void setParameterValue(String parameterHeader, Double value)
    {
        _mapping.setHeaderValue(parameterHeader, value);
    }
}
