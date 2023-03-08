package gov.usbr.wq.merlindataexchange;

import com.fasterxml.jackson.databind.exc.UnrecognizedPropertyException;
import org.xml.sax.SAXException;

import java.nio.file.Path;

public final class MerlinConfigParseException extends Exception
{
    public MerlinConfigParseException(Path configFile, Exception e)
    {
        super(buildMessage(configFile, e), e);
    }

    public MerlinConfigParseException(Path configFile, String missingFieldMessage)
    {
        super("Failed to read configuration file: " + configFile + ": " + missingFieldMessage);
    }

    private static String buildMessage(Path configFile, Exception e)
    {
        String retVal = "Failed to read configuration file: " + configFile;
        if(e instanceof UnrecognizedPropertyException)
        {
            retVal += ": " + ((UnrecognizedPropertyException)e).getOriginalMessage()
                    + ".\nExpected one of the following fields: " +((UnrecognizedPropertyException) e).getKnownPropertyIds();
        }
        else
        {
            retVal += ": " + e.getMessage();
        }
        return retVal;
    }
}
