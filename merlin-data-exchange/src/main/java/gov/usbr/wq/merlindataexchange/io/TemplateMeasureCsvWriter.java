package gov.usbr.wq.merlindataexchange.io;

import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import gov.usbr.wq.dataaccess.model.MeasureWrapper;
import gov.usbr.wq.dataaccess.model.TemplateWrapper;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static com.fasterxml.jackson.databind.MapperFeature.SORT_PROPERTIES_ALPHABETICALLY;

public final class TemplateMeasureCsvWriter extends TemplateMeasureWriter
{
    @Override
    public void write(Map<TemplateWrapper, List<MeasureWrapper>> templateMeasureMap, String exportFilePath) throws IOException
    {
        CsvMapper mapper = (CsvMapper) new CsvMapper().configure(SORT_PROPERTIES_ALPHABETICALLY,false);
        CsvSchema schema = mapper.schemaFor(FlattenedTemplateMeasure.class)
                .withHeader()
                .withColumnSeparator('|');
        super.writeInternal(templateMeasureMap, exportFilePath,mapper,schema);
    }

}
