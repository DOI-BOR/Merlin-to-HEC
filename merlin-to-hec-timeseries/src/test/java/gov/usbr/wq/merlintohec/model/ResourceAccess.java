package gov.usbr.wq.merlintohec.model;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public final class ResourceAccess
{
    private static final String USERNAME_PROPERTY = "username";
    private static final String PASSWORD_PROPERTY = "password";
    public static String getUsername()
    {
        validateSetup();
        return System.getProperty(USERNAME_PROPERTY);
    }

    private static void validateSetup()
    {
        URL userConfigUrl = ResourceAccess.class.getClassLoader().getResource("gov/usbr/wq/merlintohec/user.config");
        List<String> errors = new ArrayList<>();
        if (userConfigUrl == null)
        {
            errors.add("user.config file is missing from local file system.  Expected this at src/test/resources/gov/usbr/wq/merlintohec/user.config- see readme in the same folder for details.");
        }
        else
        {
            Properties properties = new Properties();
            try(InputStream stream = userConfigUrl.openStream())
            {
                properties.load(stream);
            }
            catch (IOException ex)
            {
                throw new UnsupportedOperationException("Unable to load properties file from " + userConfigUrl, ex);
            }

            for (String name : properties.stringPropertyNames())
            {
                System.setProperty(name, properties.getProperty(name));
            }

            String username = System.getProperty(USERNAME_PROPERTY);
            String password = System.getProperty(PASSWORD_PROPERTY);
            if (username == null)
            {
                errors.add("\tUsername missing from user.config.  Example usage: " + USERNAME_PROPERTY + "=myusername");
            }
            if (password == null)
            {
                errors.add("\tPassword missing from user.config.  Example usage: " + PASSWORD_PROPERTY + "=mypassword");
            }
        }

        if (!errors.isEmpty())
        {
            throw new UnsupportedOperationException("Unable to access Merlin, username and password are required." + System.lineSeparator() + String.join(System.lineSeparator(), errors));
        }
    }

    public static char[] getPassword()
    {
        validateSetup();
        return System.getProperty(PASSWORD_PROPERTY).toCharArray();
    }
}
