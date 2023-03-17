package gov.usbr.wq.merlindataexchange;

import gov.usbr.wq.merlindataexchange.parameters.AuthenticationParametersBuilder;
import gov.usbr.wq.merlindataexchange.parameters.MerlinParameters;
import gov.usbr.wq.merlindataexchange.parameters.MerlinParametersBuilder;
import hec.io.impl.StoreOptionImpl;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;


final class RunExtractScaleTest {

    public static void main(String[] args)
    {
        try
        {
            String workingDir = System.getProperty("user.dir");
            String libsDir = workingDir + "/build/libs";
            System.setProperty("java.library.path", libsDir);
            Field field = ClassLoader.class.getDeclaredField("sys_paths");
            field.setAccessible(true);
            field.set(null, null);
            System.out.println(System.getProperty("java.library.path"));
            MerlinDataExchangeStatus status = runExtract(args[0], Integer.parseInt(args[1]));
            if(status == MerlinDataExchangeStatus.FAILURE || status == MerlinDataExchangeStatus.AUTHENTICATION_FAILURE)
            {
                System.exit(1);
            }
            else
            {
                System.exit(0);
            }
        }
        catch(Exception e)
        {
            System.exit(-1);
        }
        finally
        {
            //safety exit to ensure this jvm eventually exits if it didn't already.
            System.exit(1);
        }
    }

    private static MerlinDataExchangeStatus runExtract(String configFileName, int process) throws IOException
    {
        String progressLogFileName = "progressLog" + process + ".log";
        String username = ResourceAccess.getUsername();
        char[] password = ResourceAccess.getPassword();
        String mockFileName = configFileName;
        Path mockXml = getMockXml(mockFileName);
        List<Path> mocks = Arrays.asList(mockXml);
        Path testDirectory = getTestDirectory();
        Path dssFile = testDirectory.resolve(mockFileName.replace(".xml", ".dss"));
        Instant start = Instant.parse("2015-02-01T12:00:00Z");
        Instant end = Instant.parse("2016-02-21T12:00:00Z");
        StoreOptionImpl storeOption = new StoreOptionImpl();
        storeOption.setRegular("0-replace-all");
        storeOption.setIrregular("0-delete_insert");
        MerlinParameters params = new MerlinParametersBuilder()
                .withWatershedDirectory(testDirectory.resolve("" + process))
                .withLogFileDirectory(testDirectory.resolve("" + process))
                .withAuthenticationParameters(new AuthenticationParametersBuilder()
                        .forUrl("https://www.grabdata2.com")
                        .setUsername(username)
                        .andPassword(password)
                        .build())
                .withStoreOption(storeOption)
                .withStart(start)
                .withEnd(end)
                .withFPartOverride("fPart")
                .build();
        DataExchangeEngine dataExchangeEngine = new MerlinDataExchangeEngineBuilder()
                .withConfigurationFiles(mocks)
                .withParameters(params)
                .withProgressListener(new TestLogProgressListener(progressLogFileName))
                .build();
        return dataExchangeEngine.runExtract().join();
    }

    private static Path getMockXml(String fileName) throws IOException
    {
        String resource = "gov/usbr/wq/merlindataexchange/" + fileName;
        URL resourceUrl = ScaleTest.class.getClassLoader().getResource(resource);
        if (resourceUrl == null)
        {
            throw new IOException("Failed to get resource: " + resource);
        }
        return new File(resourceUrl.getFile()).toPath();
    }

    private static Path getTestDirectory()
    {
        return Paths.get(System.getProperty("user.dir")).resolve("build/tmp");
    }
}
