package gov.usbr.wq.merlindataexchange.io.wq;

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
import gov.usbr.wq.merlindataexchange.configuration.DataStoreProfile;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.nio.file.Path;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;

final class CsvProfileObjectMapper extends CsvMapper
{
    private static final String WRITE_REAL_DATE_PROPERTY = "merlin.dataexchange.profile.writeRealDate";
    private static final char DELIMITER = ',';
    private static final String DATE_COL_HEADER = "Date";
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

    static void serializeDataToCsvFile(Path csvWritePath, SortedSet<ProfileSample> profileSamples) throws IOException
    {
        List<String> headers = buildHeaders(profileSamples.first());
        CsvMapper mapper = new CsvProfileObjectMapper(headers);
        CsvSchema.Builder schemaBuilder = CsvSchema.builder();
        headers.forEach(schemaBuilder::addColumn);
        CsvSchema headerSchema = schemaBuilder.build().withHeader();
        schemaBuilder.clearColumns();
        headers.forEach(c -> schemaBuilder.addColumn(c, CsvSchema.ColumnType.NUMBER_OR_STRING));
        CsvSchema schema = mapper.schemaFor(CsvProfileRow.class)
                .withColumnSeparator(DELIMITER)
                .withArrayElementSeparator(String.valueOf(DELIMITER))
                .withoutQuoteChar();
        mapper.writerFor(List.class)
                .with(headerSchema)
                .writeValue(csvWritePath.toFile(), Collections.emptyList());
        mapper.enable(CsvParser.Feature.TRIM_SPACES);
        List<CsvProfileRow> csvRows = buildCsvRows(profileSamples);
        try(Writer fileWriter = new FileWriter(csvWritePath.toFile(), true);
            SequenceWriter sequenceWriter = mapper.writerFor(CsvProfileRow.class)
                    .with(schema)
                    .writeValues(fileWriter))
        {
            sequenceWriter.writeAll(csvRows);
        }
    }

    //deserialization is current used for testing only, but could be tweaked in future if we ever want to POST data.
    static SortedSet<ProfileSample> deserializeDataFromCsv(Path csvFile) throws IOException, InvalidProfileCsvException
    {
        CsvMapper mapper = new CsvMapper();
        CsvSchema schema = CsvSchema.emptySchema().withHeader()
                .withColumnSeparator(DELIMITER)
                .withoutEscapeChar();
        try(MappingIterator<JsonNode> nodeIterator = mapper.readerFor(JsonNode.class).with(schema)
                .readValues(csvFile.toFile());
            BufferedReader reader = new BufferedReader(new FileReader(csvFile.toFile())))
        {
            List<String> headers = new ArrayList<>();
            String headerLine = reader.readLine();
            if (headerLine != null)
            {
                String[] headersArr = headerLine.split(",");
                for(int i=0; i < headersArr.length; i++)
                {
                    headersArr[i] = headersArr[i].replace("\"", "");
                }
                headers = Arrays.asList(headersArr);
            }
            validateCsv(csvFile, headers);
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
                    if(value != null)
                    {
                        double d = value.asDouble();
                        row.setParameterValue(key, d);
                    }
                }
                rows.add(row);
            }
            return buildProfileSamplesFromRows(headers, rows, csvFile);
        }
    }

    private static void validateCsv(Path csvFile, List<String> headers) throws InvalidProfileCsvException
    {
        if(headers.isEmpty())
        {
            throw new InvalidProfileCsvException(csvFile, "Missing headers");
        }
        if(!headers.get(0).equalsIgnoreCase(DATE_COL_HEADER))
        {
            throw new InvalidProfileCsvException(csvFile, "Expected first column header to be " + DATE_COL_HEADER + ". But was " + headers.get(0));
        }
        validateDepthColumnExists(csvFile, headers);
    }

    private static void validateDepthColumnExists(Path csvFile, List<String> headers) throws InvalidProfileCsvException
    {
        boolean depthPresent = false;
        for(String header : headers)
        {
            if(header.toLowerCase().contains(DataStoreProfile.DEPTH.toLowerCase()))
            {
                depthPresent = true;
                break;
            }
        }
        if(!depthPresent)
        {
            throw new InvalidProfileCsvException(csvFile, "Missing depth data");
        }
    }

    private static SortedSet<ProfileSample> buildProfileSamplesFromRows(List<String> headers, List<CsvProfileRow> rows, Path csvFile)
    {
        String station = parseStationFromFileName(csvFile);
        SortedSet<ProfileSample> retVal = new TreeSet<>(Comparator.comparing(ProfileSample::getDateTime));
        if(!rows.isEmpty())
        {
            List<ProfileConstituent> constituentDataList = new ArrayList<>();
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
                        Double value = mapping.getHeaderToValuesMap().get(header);
                        valuesForColumn.add(value);
                    }
                    ProfileConstituent constituentData = new ProfileConstituent(param, valuesForColumn, new ArrayList<>(), unit);
                    constituentDataList.add(constituentData);
                }
            }
            List<ZonedDateTime> dates = new ArrayList<>();
            for(CsvProfileRow row : rows)
            {
                dates.add(row.getDate());
            }
            retVal = ProfileDataConverter.splitDataIntoProfileSamples(constituentDataList, dates, station, false, false);
        }
        return retVal;
    }

    private static String parseStationFromFileName(Path csvFile)
    {
        String retVal = null;
        String[] dashSplit = csvFile.toString().split("-");
        if(dashSplit.length > 2)
        {
            retVal = dashSplit[dashSplit.length - 2];
        }
        return retVal;
    }

    static List<String> buildHeaders(ProfileSample sample)
    {
        List<String> retVal = new ArrayList<>();
        retVal.add(DATE_COL_HEADER);
        for(ProfileConstituent profileConstituent : sample.getConstituents())
        {
            retVal.add(profileConstituent.getParameter() + "(" + profileConstituent.getUnit() + ")");
        }
        boolean writeRealDate = Boolean.getBoolean(WRITE_REAL_DATE_PROPERTY);
        if(writeRealDate)
        {
            retVal.add("Real Date");
        }
        return retVal;
    }

    private static List<CsvProfileRow> buildCsvRows(SortedSet<ProfileSample> profileSamples)
    {
        List<CsvProfileRow> retVal = new ArrayList<>();
        for(ProfileSample depthTempProfileSample : profileSamples)
        {
            List<CsvProfileRow> rowsForSample = new ArrayList<>();
            ZonedDateTime dateTime = depthTempProfileSample.getDateTime();
            for(int i = 0; i < depthTempProfileSample.getConstituents().get(0).getDataValues().size(); i++)
            {
                CsvProfileRow row = new CsvProfileRow();
                row.setDate(dateTime);
                rowsForSample.add(row);
            }
            for (ProfileConstituent data : depthTempProfileSample.getConstituents())
            {
                for (int i = 0; i < data.getDataValues().size(); i++)
                {
                    CsvProfileRow csvRow = rowsForSample.get(i);
                    double value = data.getDataValues().get(i);
                    String unit = data.getUnit();
                    String parameterName = data.getParameter();
                    csvRow.setParameterValue(parameterName + "(" + unit + ")", value);
                    if(Boolean.getBoolean(WRITE_REAL_DATE_PROPERTY))
                    {
                        csvRow.setRealDate(data.getDateValues().get(i));
                    }
                }
            }
            retVal.addAll(rowsForSample);
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
                if (mapping.getHeaderToValuesMap().containsKey(key))
                {
                    sortedMap.put(key, mapping.getHeaderToValuesMap().get(key));
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
