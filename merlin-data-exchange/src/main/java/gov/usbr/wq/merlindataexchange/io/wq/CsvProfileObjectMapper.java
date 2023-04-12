package gov.usbr.wq.merlindataexchange.io.wq;

import com.fasterxml.jackson.core.JacksonException;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.json.JsonReadFeature;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.MappingIterator;
import com.fasterxml.jackson.databind.SequenceWriter;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvParser;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.nio.file.Path;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

final class CsvProfileObjectMapper extends CsvMapper
{
    CsvProfileObjectMapper(List<String> headers)
    {
        configureMapper(headers);
    }

    private void configureMapper(List<String> headers)
    {
        registerModule(new JavaTimeModule());
        findAndRegisterModules();
        SimpleModule simpleModule = new SimpleModule();
        simpleModule.addSerializer(ZonedDateTime.class, new ZonedDateTimeSerializer());
        simpleModule.addDeserializer(ZonedDateTime.class, new ZonedDateTimeDeserializer());
        simpleModule.addSerializer(Double.class, new DoubleSerializer());
        simpleModule.addSerializer(CsvDataMapping.class, new CsvDataMappingSerializer(headers));
        registerModule(simpleModule);
        configure(JsonReadFeature.ALLOW_UNESCAPED_CONTROL_CHARS.mappedFeature(), true);
        configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    }

    static void serializeDataToCsvFile(Path csvWritePath, ProfileSample depthTempProfileSample) throws IOException
    {
        List<String> headers = buildHeaders(depthTempProfileSample);
        CsvMapper mapper = new CsvProfileObjectMapper(headers);
        CsvSchema.Builder schemaBuilder = CsvSchema.builder();
        headers.forEach(schemaBuilder::addColumn);
        CsvSchema headerSchema = schemaBuilder.build().withHeader();
        schemaBuilder.clearColumns();
        headers.forEach(c -> schemaBuilder.addColumn(c, CsvSchema.ColumnType.NUMBER_OR_STRING));
        CsvSchema schema = mapper.schemaFor(CsvProfileRow.class)
                .withColumnSeparator(',')
                .withArrayElementSeparator(",")
                .withoutQuoteChar();
        mapper.writerFor(List.class)
                .with(headerSchema)
                .writeValue(csvWritePath.toFile(), Collections.emptyList());
        mapper.enable(CsvParser.Feature.TRIM_SPACES).enable(CsvParser.Feature.ALLOW_TRAILING_COMMA);
        List<CsvProfileRow> csvRows = buildCsvRows(depthTempProfileSample);
        try(Writer fileWriter = new FileWriter(csvWritePath.toFile(), true);
            SequenceWriter sequenceWriter = mapper.writerFor(CsvProfileRow.class)
                    .with(schema)
                    .writeValues(fileWriter))
        {
            sequenceWriter.writeAll(csvRows);
        }
    }

    static ProfileSample deserializeDataFromCsv(Path csvFile) throws IOException
    {
        CsvMapper mapper = new CsvMapper();
        CsvSchema schema = CsvSchema.emptySchema().withHeader()
                .withColumnSeparator(',')
                .withoutEscapeChar();
        // Step 4: Create MappingIterator
        try(MappingIterator<JsonNode> nodeIterator = mapper.readerFor(JsonNode.class).with(schema)
                .readValues(csvFile.toFile()))
        {
            List<String> headers = ((CsvSchema)nodeIterator.getParserSchema()).getColumnNames();
            List<CsvProfileRow> rows = new ArrayList<>();
            while (nodeIterator.hasNext())
            {
                CsvProfileRow row = new CsvProfileRow();
                // Get the data for the current row
                JsonNode node = nodeIterator.next();
                // Read the date column from the first element in the row
                String dateString = node.get(headers.get(0)).asText();
                row.setDate(ZonedDateTime.parse(dateString, DateTimeFormatter.ISO_OFFSET_DATE_TIME));

                // Read the rest of the data columns into the CsvDataMapping object
                for (int i = 1; i < headers.size(); i++)
                {
                    String key = headers.get(i);
                    JsonNode value = node.get(key);
                    double d = value.asDouble();
                    row.setParameterValue(key, d);
                }
                rows.add(row);
            }
            return buildProfileSampleFromRows(headers, rows);
        }
    }

    private static ProfileSample buildProfileSampleFromRows(List<String> headers, List<CsvProfileRow> rows)
    {
        ProfileSample retVal = null;
        if(!rows.isEmpty())
        {
            List<ProfileConstituentData> constituentDataList = new ArrayList<>();
            for(String header : headers)
            {
                if(header.contains("("))
                {
                    int startUnitIndex = header.indexOf("(");
                    int endIndex = header.indexOf(")");
                    String unit = header.substring(startUnitIndex +1, endIndex);
                    String param = header.substring(0, startUnitIndex);
                    List<Double> valuesForColumn = new ArrayList<>();
                    for(CsvProfileRow row : rows)
                    {
                        CsvDataMapping mapping = row.getMapping();
                        Double value = mapping.getParameterValues().get(header);
                        valuesForColumn.add(value);
                    }
                    ProfileConstituentData constituentData = new ProfileConstituentData(param, valuesForColumn, unit);
                    constituentDataList.add(constituentData);
                }
            }
            retVal = new ProfileSample(rows.get(0).getDate(), constituentDataList);
        }
        return retVal;
    }

    static List<String> buildHeaders(ProfileSample sample)
    {
        List<String> retVal = new ArrayList<>();
        retVal.add("Date");
        for(ProfileConstituentData profileConstituentData : sample.getConstituentDataList())
        {
            retVal.add(profileConstituentData.getParameter() + "(" + profileConstituentData.getUnit() + ")");
        }
        return retVal;
    }

    private static List<CsvProfileRow> buildCsvRows(ProfileSample depthTempProfileSample)
    {
        ZonedDateTime dateTime = depthTempProfileSample.getDateTime();
        List<CsvProfileRow> retVal = new ArrayList<>();
        for(int i=0; i < depthTempProfileSample.getConstituentDataList().get(0).getDataValues().size(); i++)
        {
            CsvProfileRow row = new CsvProfileRow();
            row.setDate(dateTime);
            retVal.add(row);
        }
        for (ProfileConstituentData data : depthTempProfileSample.getConstituentDataList())
        {
            for (int i = 0; i < data.getDataValues().size(); i++)
            {
                CsvProfileRow csvRow = retVal.get(i);
                double value = data.getDataValues().get(i);
                String unit = data.getUnit();
                String parameterName = data.getParameter();
                csvRow.setParameterValue(parameterName + "(" + unit + ")", value);
            }
        }
        return retVal;
    }

    private static class ZonedDateTimeSerializer extends JsonSerializer<ZonedDateTime>
    {
        @Override
        public void serialize(ZonedDateTime value, JsonGenerator gen, SerializerProvider provider) throws IOException
        {
            gen.writeString(value.format(DateTimeFormatter.ISO_OFFSET_DATE_TIME));
        }
    }

    private static class ZonedDateTimeDeserializer extends JsonDeserializer<ZonedDateTime>
    {
        @Override
        public ZonedDateTime deserialize(JsonParser p, DeserializationContext ctxt) throws IOException
        {
            String dateStr = p.getText().trim();
            return ZonedDateTime.parse(dateStr, DateTimeFormatter.ISO_OFFSET_DATE_TIME);
        }

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
            }
            else
            {
                int intValue = value.intValue();
                if (intValue == value)
                {
                    gen.writeNumber(intValue);
                }
                else
                {
                    gen.writeNumber(value);
                }
            }

        }
    }

    private static class CsvDataMappingSerializer extends JsonSerializer<CsvDataMapping>
    {

        private final List<String> _headers;

        CsvDataMappingSerializer(List<String> headers)
        {
            _headers = headers;
        }
        @Override
        public void serialize(CsvDataMapping mapping, JsonGenerator gen, SerializerProvider serializers) throws IOException
        {
            Map<String, Double> sortedMap = new LinkedHashMap<>();
            for (String key : _headers)
            {
                if (mapping.getParameterValues().containsKey(key))
                {
                    sortedMap.put(key, mapping.getParameterValues().get(key));
                }
            }
            gen.writeStartArray();
            for (Double value : sortedMap.values())
            {
                gen.writeNumber(value);
            }
            gen.writeEndArray();
        }
    }
}
