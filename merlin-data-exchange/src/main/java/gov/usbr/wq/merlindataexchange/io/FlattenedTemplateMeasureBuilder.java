package gov.usbr.wq.merlindataexchange.io;

import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import gov.usbr.wq.dataaccess.json.Measure;
import gov.usbr.wq.dataaccess.json.Template;
import gov.usbr.wq.dataaccess.mapper.MerlinObjectMapper;
import gov.usbr.wq.dataaccess.model.MeasureWrapper;
import gov.usbr.wq.dataaccess.model.TemplateWrapper;

import java.io.IOException;

public class FlattenedTemplateMeasureBuilder
{

    private TemplateWrapper _templateWraper;
    private MeasureWrapper _measureWrapper;

    public FlattenedTemplateMeasureBuilder()
    {
    }

    public FlattenedMeasureBuilder withTemplate(TemplateWrapper templateWrapper) throws IOException
    {
        _templateWraper = templateWrapper;
        return new FlattenedMeasureBuilder();
    }

    public class FlattenedMeasureBuilder
    {
        public FlattenedBuilder withMeasure(MeasureWrapper measureWrapper) throws IOException
        {
            _measureWrapper = measureWrapper;
            return new FlattenedBuilder();
        }
    }

    public class FlattenedBuilder
    {
        public FlattenedTemplateMeasure build()
        {
            return new FlattenedTemplateMeasure(_templateWraper, _measureWrapper);
        }
    }
}
