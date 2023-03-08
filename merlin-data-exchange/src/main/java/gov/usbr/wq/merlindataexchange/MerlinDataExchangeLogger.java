package gov.usbr.wq.merlindataexchange;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Pattern;

final class MerlinDataExchangeLogger
{

    private static final Logger LOGGER = Logger.getLogger(MerlinDataExchangeLogger.class.getName());
    private static final Pattern LOG_PATTERN = Pattern.compile(".log$");
    static final String SEPARATOR = "------------------------------------------------------";
    private final StringBuilder _header = new StringBuilder();
    private final List<MerlinDataExchangeLogBody> _logBodies = new ArrayList<>();

    private final StringBuilder _footer = new StringBuilder();
    private final Path _logFile;

    MerlinDataExchangeLogger(Path logFile)
    {
        _logFile = logFile;
    }

    synchronized void logToHeader(String logMessage)
    {
        _header.append(logMessage);
        _header.append("\n");
    }

    synchronized void logBody(MerlinDataExchangeLogBody body)
    {
        _logBodies.add(body);
    }

    synchronized void logToFooter(String logMessage)
    {
        _footer.append(logMessage);
        _footer.append("\n");
    }

    synchronized void writeLog()
    {
        try
        {
            if(_logFile.toFile().exists())
            {
                Path bakFile = Paths.get(_logFile.toString().replaceAll(LOG_PATTERN.pattern(), ".bak"));
                Files.deleteIfExists(bakFile);
                Files.copy(_logFile, bakFile);
                Files.delete(_logFile);
            }
            Files.createFile(_logFile);
            String stringToWrite = buildLogString();
            byte[] bytes = stringToWrite.getBytes();
            Files.write(_logFile, bytes);
        }
        catch (IOException e)
        {
            LOGGER.log(Level.WARNING, e, () -> "Failed to write log to file: " + _logFile);
        }
    }

    private String buildLogString()
    {
        StringBuilder retVal = new StringBuilder(_header);
        retVal.append(SEPARATOR);
        retVal.append("\n");
        retVal.append(SEPARATOR);
        retVal.append("\n");
        _logBodies.forEach(body ->
        {
            retVal.append(body.getLog());
            retVal.append(SEPARATOR);
            retVal.append("\n");
        });
        retVal.append(SEPARATOR);
        retVal.append("\n");
        retVal.append(_footer);
        return retVal.toString();
    }

    Path getLogFile()
    {
        return _logFile;
    }

}
