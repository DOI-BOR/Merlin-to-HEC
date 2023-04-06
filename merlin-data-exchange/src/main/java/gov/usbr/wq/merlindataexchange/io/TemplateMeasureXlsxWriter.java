package gov.usbr.wq.merlindataexchange.io;

import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import com.github.sett4.dataformat.xlsx.XlsxMapper;
import gov.usbr.wq.dataaccess.model.MeasureWrapper;
import gov.usbr.wq.dataaccess.model.TemplateWrapper;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public final class TemplateMeasureXlsxWriter extends TemplateMeasureWriter
{
    @Override
    public void write(Map<TemplateWrapper, List<MeasureWrapper>> templateMeasureMap, String exportFilePath) throws IOException
    {
        XlsxMapper mapper = new XlsxMapper();
        CsvSchema schema = mapper.schemaFor(FlattenedTemplateMeasure.class)
                .withHeader()
                .withColumnSeparator('|');
        super.writeInternal(templateMeasureMap, exportFilePath,mapper,schema);
    }
}
