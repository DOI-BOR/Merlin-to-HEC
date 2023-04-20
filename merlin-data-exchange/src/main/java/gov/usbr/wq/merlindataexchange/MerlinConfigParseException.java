package gov.usbr.wq.merlindataexchange;

import com.fasterxml.jackson.databind.exc.UnrecognizedPropertyException;
import org.xml.sax.SAXParseException;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;

import static java.util.stream.Collectors.toList;

public class MerlinConfigParseException extends Exception
{
    public MerlinConfigParseException(Path configFile, Exception e)
    {
        super(buildMessage(configFile, e), e);
    }

    public MerlinConfigParseException(Path configFile, String missingFieldMessage)
    {
        super("Failed to read configuration file: " + configFile + ":\n" + missingFieldMessage);
    }

    private static String buildMessage(Path configFile, Exception e)
    {
        String retVal = "Failed to read configuration file: " + configFile;
        if(e instanceof UnrecognizedPropertyException)
        {
            retVal += ":\n" + ((UnrecognizedPropertyException)e).getOriginalMessage()
                    + ".\nExpected one of the following fields: " + getExpectedFields((UnrecognizedPropertyException) e);
        }
        else if(e instanceof SAXParseException)
        {
            SAXParseException saxException = (SAXParseException) e;
            retVal += ":\n" + saxException.getMessage() + "\n"
                    + "line: " + saxException.getLineNumber() + ", column: " + saxException.getColumnNumber();
        }
        else
        {
            retVal += ":\n" + e.getMessage();
        }
        return retVal;
    }

    private static String getExpectedFields(UnrecognizedPropertyException e)
    {
        List<String> fieldNames = Arrays.stream(e.getReferringClass().getDeclaredFields())
                .map(MerlinConfigParseException::getFieldAsPropertyName)
                .collect(toList());
        List<String> methodNames = Arrays.stream(e.getReferringClass().getDeclaredMethods())
                .map(MerlinConfigParseException::getMethodAsPropertyName)
                .collect(toList());
        return e.getKnownPropertyIds().stream()
                .filter(id -> !fieldNames.contains(id.toString()) && !methodNames.contains(id.toString()))
                .collect(toList()).toString();
    }

    private static String getMethodAsPropertyName(Method method)
    {
        String retVal = method.getName();
        if(retVal.startsWith("get") || retVal.startsWith("set"))
        {
            retVal = retVal.substring(3);
            char[] chars = retVal.toCharArray();
            chars[0] = Character.toLowerCase(chars[0]);
            retVal = new String(chars);
        }
        return retVal;
    }

    private static String getFieldAsPropertyName(Field f)
    {
        String retVal = f.getName();
        if(retVal.startsWith("_"))
        {
            retVal = retVal.substring(1);
        }
        return retVal;
    }

}
