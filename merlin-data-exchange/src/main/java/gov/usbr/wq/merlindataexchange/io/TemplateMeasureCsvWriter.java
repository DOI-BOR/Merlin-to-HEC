package gov.usbr.wq.merlindataexchange.io;

import com.fasterxml.jackson.core.JacksonException;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.json.JsonReadFeature;
import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.github.sett4.dataformat.xlsx.XlsxMapper;
import gov.usbr.wq.dataaccess.mapper.MerlinObjectMapper;
import gov.usbr.wq.dataaccess.model.MeasureWrapper;
import gov.usbr.wq.dataaccess.model.TemplateWrapper;

import java.io.File;
import java.io.IOException;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class TemplateMeasureCsvWriter {
    public void writeToCsv(Map<TemplateWrapper, List<MeasureWrapper>> templateMeasureMap, String exportFilePath) throws IOException {
        List<FlattenedTemplateMeasure> flattenedTemplateMeasures = flattenTemplateMeasureMap(templateMeasureMap);
//        CsvMapper mapper = new CsvMapper();
        XlsxMapper mapper = new XlsxMapper();
        mapper.registerModule(new JavaTimeModule());
        mapper.findAndRegisterModules();
        SimpleModule simpleModule = new SimpleModule();
        simpleModule.addSerializer(OffsetDateTime.class, new JsonSerializer<OffsetDateTime>() {
            public void serialize(OffsetDateTime offsetDateTime, JsonGenerator jsonGenerator, SerializerProvider serializerProvider) throws IOException {
                jsonGenerator.writeString(DateTimeFormatter.ISO_OFFSET_DATE_TIME.format(offsetDateTime));
            }
        });
        simpleModule.addDeserializer(OffsetDateTime.class, new JsonDeserializer<OffsetDateTime>() {
            public OffsetDateTime deserialize(JsonParser p, DeserializationContext ctxt) throws IOException, JacksonException {
                String dateStr = p.getText().trim();
                return OffsetDateTime.parse(dateStr, DateTimeFormatter.ISO_OFFSET_DATE_TIME);
            }
        });
        simpleModule.addSerializer(Double.class, new TemplateMeasureCsvWriter.DoubleSerializer());
        mapper.registerModule(simpleModule);
        mapper.configure(JsonReadFeature.ALLOW_UNESCAPED_CONTROL_CHARS.mappedFeature(), true);
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

        CsvSchema csvSchema = mapper.schemaFor(FlattenedTemplateMeasure.class)
                .withHeader()
                .withColumnSeparator('|' );

        mapper.writer(csvSchema).writeValue(new File(exportFilePath), flattenedTemplateMeasures.toArray());
    }

    private static class DoubleSerializer extends JsonSerializer<Double> {
        private DoubleSerializer() {
        }

        public void serialize(Double value, JsonGenerator gen, SerializerProvider serializers) throws IOException {
            if (value == null) {
                gen.writeNull();
            } else {
                int intValue = value.intValue();
                if ((double)intValue == value) {
                    gen.writeNumber(intValue);
                } else {
                    gen.writeNumber(value);
                }
            }

        }
    }

    private List<FlattenedTemplateMeasure> flattenTemplateMeasureMap(Map<TemplateWrapper, List<MeasureWrapper>> templateMeasureMap) throws IOException {
        List<FlattenedTemplateMeasure> flattenedTemplateMeasures = new ArrayList<>();
        for (Map.Entry<TemplateWrapper, List<MeasureWrapper>> entry : templateMeasureMap.entrySet()) {
            TemplateWrapper templateWrapper = entry.getKey();
            List<MeasureWrapper> measureWrappers = entry.getValue();
            for (MeasureWrapper measureWrapper : measureWrappers) {
                FlattenedTemplateMeasure flattenedTemplateMeasure = new FlattenedTemplateMeasureBuilder()
                        .withTemplate(templateWrapper)
                        .withMeasure(measureWrapper)
                        .build();
                flattenedTemplateMeasures.add(flattenedTemplateMeasure);
            }
        }
        return flattenedTemplateMeasures;
    }

}
