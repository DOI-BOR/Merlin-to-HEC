package gov.usbr.wq.merlindataexchange.io.wq;

import java.time.ZonedDateTime;

final class CsvProfileRow
{
    private ZonedDateTime _date;
    private CsvDataMapping _mapping = new CsvDataMapping();

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
        _mapping.setParameterValue(parameterHeader, value);
    }
}
