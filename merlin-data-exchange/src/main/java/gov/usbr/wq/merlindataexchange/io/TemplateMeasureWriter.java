package gov.usbr.wq.merlindataexchange.io;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.json.JsonReadFeature;
import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import gov.usbr.wq.dataaccess.model.MeasureWrapper;
import gov.usbr.wq.dataaccess.model.TemplateWrapper;

import java.io.File;
import java.io.IOException;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public abstract class TemplateMeasureWriter
{
    public abstract void write(Map<TemplateWrapper, List<MeasureWrapper>> templateMeasureMap, String exportFilePath) throws IOException;

    protected List<FlattenedTemplateMeasure> flattenTemplateMeasureMap(Map<TemplateWrapper, List<MeasureWrapper>> templateMeasureMap) throws IOException
    {
        List<FlattenedTemplateMeasure> flattenedTemplateMeasures = new ArrayList<>();
        for (Map.Entry<TemplateWrapper, List<MeasureWrapper>> entry : templateMeasureMap.entrySet())
        {
            TemplateWrapper templateWrapper = entry.getKey();
            List<MeasureWrapper> measureWrappers = entry.getValue();
            for (MeasureWrapper measureWrapper : measureWrappers)
            {
                FlattenedTemplateMeasure flattenedTemplateMeasure = new FlattenedTemplateMeasureBuilder()
                        .withTemplate(templateWrapper)
                        .withMeasure(measureWrapper)
                        .build();
                flattenedTemplateMeasures.add(flattenedTemplateMeasure);
            }
        }
        return flattenedTemplateMeasures;
    }

    protected void writeInternal(Map<TemplateWrapper, List<MeasureWrapper>> templateMeasureMap, String exportFilePath, ObjectMapper mapper, CsvSchema schema) throws IOException
    {
        List<FlattenedTemplateMeasure> flattenedTemplateMeasures = flattenTemplateMeasureMap(templateMeasureMap);
        mapper.registerModule(new JavaTimeModule());
        mapper.findAndRegisterModules();
        SimpleModule simpleModule = new SimpleModule();
        simpleModule.addSerializer(OffsetDateTime.class, new JsonSerializer<OffsetDateTime>()
        {
            public void serialize(OffsetDateTime offsetDateTime, JsonGenerator jsonGenerator, SerializerProvider serializerProvider) throws IOException
            {
                jsonGenerator.writeString(DateTimeFormatter.ISO_OFFSET_DATE_TIME.format(offsetDateTime));
            }
        });
        simpleModule.addDeserializer(OffsetDateTime.class, new JsonDeserializer<OffsetDateTime>()
        {
            public OffsetDateTime deserialize(JsonParser p, DeserializationContext ctxt) throws IOException
            {
                String dateStr = p.getText().trim();
                return OffsetDateTime.parse(dateStr, DateTimeFormatter.ISO_OFFSET_DATE_TIME);
            }
        });
        simpleModule.addSerializer(Double.class, new TemplateMeasureCsvWriter.DoubleSerializer());
        mapper.registerModule(simpleModule);
        mapper.configure(JsonReadFeature.ALLOW_UNESCAPED_CONTROL_CHARS.mappedFeature(), true);
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

        mapper.writer(schema).writeValue(new File(exportFilePath), flattenedTemplateMeasures.toArray());
    }

    protected static class DoubleSerializer extends JsonSerializer<Double>
    {
        protected DoubleSerializer()
        {
        }

        public void serialize(Double value, JsonGenerator gen, SerializerProvider serializers) throws IOException
        {
            if (value == null)
            {
                gen.writeNull();
            } else
            {
                int intValue = value.intValue();
                if ((double) intValue == value)
                {
                    gen.writeNumber(intValue);
                } else
                {
                    gen.writeNumber(value);
                }
            }

        }
    }
}
