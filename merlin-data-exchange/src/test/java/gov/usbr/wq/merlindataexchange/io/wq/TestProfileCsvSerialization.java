package gov.usbr.wq.merlindataexchange.io.wq;

import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;

final class TestProfileCsvSerialization
{
    @Test
    void testSerialization() throws IOException, InvalidProfileCsvException
    {
        Path fileToWriteTo = getTestDirectory().resolve("testProfileSerialization.csv");
        Files.deleteIfExists(fileToWriteTo);
        ZonedDateTime dateTime = ZonedDateTime.parse("2023-04-10T00:00:00-08:00", DateTimeFormatter.ISO_OFFSET_DATE_TIME);
        List<Double> temps = Arrays.asList(10.0, 11.0);
        List<Double> depths = Arrays.asList(1.0, 2.0);
        String tempUnits = "C";
        String depthUnits = "ft";
        List<ProfileConstituent> profileConstituentData = new ArrayList<>();
        profileConstituentData.add(new ProfileConstituent("Depth", depths, new ArrayList<>(), depthUnits));
        profileConstituentData.add(new ProfileConstituent("Temp-Water", temps, new ArrayList<>(), tempUnits));
        ProfileSample sample = new ProfileSample(dateTime, profileConstituentData);
        SortedSet<ProfileSample> samples = new TreeSet<>(Comparator.comparing(ProfileSample::getDateTime));
        samples.add(sample);
        CsvProfileObjectMapper.serializeDataToCsvFile(fileToWriteTo, samples);
        ProfileSample returnedSample = CsvProfileObjectMapper.deserializeDataFromCsv(fileToWriteTo).first();
        assertEquals(sample, returnedSample);
    }

    @Test
    void testSerializationAgainstSource() throws IOException, InvalidProfileCsvException
    {
        Path sourceCsvFile = getFile("merlin_mock_profile.csv");
        List<String> csvLines = readCsvLines(sourceCsvFile);
        SortedSet<ProfileSample> profiles = CsvProfileObjectMapper.deserializeDataFromCsv(sourceCsvFile);
        int row = 1;
        for(ProfileSample profileSample : profiles)
        {
            String dateTime = profileSample.getDateTime().format(DateTimeFormatter.ISO_OFFSET_DATE_TIME);
            List<ProfileConstituent> constituents = profileSample.getConstituents();
            for(int i=0; i < constituents.get(0).getDataValues().size(); i++)
            {
                List<Double> values = new ArrayList<>();
                for(ProfileConstituent constituent : constituents)
                {
                    values.add(constituent.getDataValues().get(i));
                }
                String profileLine = dateTime + "," + values.stream().map(Object::toString).collect(Collectors.joining(","));
                assertEquals(profileLine, csvLines.get(row));
                row++;
            }
        }
        Path serializationOutputFile = getTestDirectory().resolve("serializationTest.csv");
        CsvProfileObjectMapper.serializeDataToCsvFile(serializationOutputFile, profiles);
        List<String> serializedLines = readCsvLines(serializationOutputFile);
        assertEquals(csvLines, serializedLines);
    }

    private List<String> readCsvLines(Path sourceCsvFile) throws IOException
    {
        List<String> retVal = new ArrayList<>();
        try (BufferedReader br = new BufferedReader(new FileReader(sourceCsvFile.toFile())))
        {
            String line;
            while ((line = br.readLine()) != null)
            {
                if(line.endsWith(","))
                {
                    line = line.substring(0, line.length() - 1);
                }
                retVal.add(line);
            }
        }
        return retVal;
    }

    private Path getTestDirectory()
    {
        return Paths.get(System.getProperty("user.dir")).resolve("build/tmp");
    }

    private Path getFile(String fileName) throws IOException
    {
        String resource = "gov/usbr/wq/merlindataexchange/io/wq/" + fileName;
        URL resourceUrl = getClass().getClassLoader().getResource(resource);
        if (resourceUrl == null)
        {
            throw new IOException("Failed to get resource: " + resource);
        }
        return new File(resourceUrl.getFile()).toPath();
    }

}
