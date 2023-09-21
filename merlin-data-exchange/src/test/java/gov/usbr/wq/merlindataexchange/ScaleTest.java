package gov.usbr.wq.merlindataexchange;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

final class ScaleTest
{

    private static final Logger LOGGER = Logger.getLogger(ScaleTest.class.getName());
    private static final int NUMBER_OF_SIMULTANEOUS_RUNS = 10;

    public static void main(String[] args) throws Exception {
        String configFile = "merlin_mock_config_dx.xml";
        ClassLoader.getSystemClassLoader().setDefaultAssertionStatus(true);
        String classpath = System.getProperty("java.class.path");
        String className = RunExtractScaleTest.class.getName();

        List<ProcessBuilder> processBuilderList = new ArrayList<>(NUMBER_OF_SIMULTANEOUS_RUNS);
        for(int i=0 ; i < NUMBER_OF_SIMULTANEOUS_RUNS; i++)
        {
            ProcessBuilder pb = new ProcessBuilder("java", "-cp", classpath, className, configFile, "" + (i+1));
            pb.redirectError(ProcessBuilder.Redirect.INHERIT);
            pb.inheritIO();
            pb.directory(new File("."));
            processBuilderList.add(pb);
        }
        List<Process> processes = new ArrayList<>();
        for(ProcessBuilder pb : processBuilderList)
        {
            Process process = pb.start();
            processes.add(process);
        }
        boolean errorOccurred = false;
        for(Process process : processes)
        {
            int exitCode = process.waitFor();

            if(exitCode != 0)
            {
                LOGGER.severe("Process did not complete as expected");
                errorOccurred = true;
            }
            else
            {
                LOGGER.info("Process completed successfully");
            }
        }
        if(errorOccurred)
        {
            throw new AssertionError("Error Occurred. Test Failed");
        }
    }
}
