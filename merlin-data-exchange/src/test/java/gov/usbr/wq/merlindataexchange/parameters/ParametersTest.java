package gov.usbr.wq.merlindataexchange.parameters;

import hec.io.impl.StoreOptionImpl;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

final class ParametersTest
{
    @Test
    void testBuildAuthenticationParameters()
    {
        AuthenticationParameters authParams = new AuthenticationParametersBuilder()
                .forUrl("https://www.grabdata2.com")
                .setUsername("user")
                .andPassword("password".toCharArray())
                .build();
        assertEquals("https://www.grabdata2.com", authParams.getUrl());
        assertEquals("user", authParams.getUsernamePassword().getUsername());
        assertEquals("password", new String(authParams.getUsernamePassword().getPassword()));
    }

    @Test
    void testBuildMerlinParameters() throws UsernamePasswordNotFoundException {
        Path workingDir = Paths.get(System.getProperty("user.dir"));
        Instant start = Instant.parse("2019-01-01T08:00:00Z");
        Instant end = Instant.parse("2022-08-30T08:00:00Z");
        StoreOptionImpl storeOption = new StoreOptionImpl();
        storeOption.setRegular("0-replace-all");
        storeOption.setIrregular("0-delete_insert");
        AuthenticationParameters authParams = new AuthenticationParametersBuilder()
                .forUrl("https://www.grabdata2.com")
                .setUsername("user")
                .andPassword("password".toCharArray())
                .build();
        Path logDir = workingDir.resolve("log");
        MerlinParameters params = new MerlinParametersBuilder()
                .withWatershedDirectory(workingDir)
                .withLogFileDirectory(logDir)
                .withStart(start)
                .withEnd(end)
                .withStoreOption(storeOption)
                .withFPartOverride("fPart")
                .withAuthenticationParameters(authParams)
                .build();
        assertEquals(params.getWatershedDirectory(), workingDir);
        assertEquals(params.getLogFileDirectory(), logDir);
        assertEquals(params.getStart(), start);
        assertEquals(params.getEnd(), end);
        assertEquals(params.getStoreOption(), storeOption);
        assertEquals(params.getFPartOverride(), "fPart");
        UsernamePasswordHolder usernamePassword = params.getUsernamePasswordForUrl("https://www.grabdata2.com");
        assertEquals("user", usernamePassword.getUsername());
        assertEquals("password", new String(usernamePassword.getPassword()));
        assertThrows(UsernamePasswordNotFoundException.class, () -> params.getUsernamePasswordForUrl("bleh"));
    }

    @Test
    void testGetAuthParameters() throws UsernamePasswordNotFoundException
    {
        Path workingDir = Paths.get(System.getProperty("user.dir"));
        Instant start = Instant.parse("2019-01-01T08:00:00Z");
        Instant end = Instant.parse("2022-08-30T08:00:00Z");
        StoreOptionImpl storeOption = new StoreOptionImpl();
        storeOption.setRegular("0-replace-all");
        storeOption.setIrregular("0-delete_insert");
        AuthenticationParameters authParams = new AuthenticationParametersBuilder()
                .forUrl("https://www.grabdata2.com")
                .setUsername("user")
                .andPassword("password".toCharArray())
                .build();
        MerlinParameters params = new MerlinParametersBuilder()
                .withWatershedDirectory(workingDir)
                .withLogFileDirectory(workingDir)
                .withStart(start)
                .withEnd(end)
                .withStoreOption(storeOption)
                .withFPartOverride("fPart")
                .withAuthenticationParameters(authParams)
                .build();
        UsernamePasswordHolder usernamePassword = params.getUsernamePasswordForUrl("https://www.grabdata2.com");
        assertEquals("user", usernamePassword.getUsername());
        assertEquals("password", new String(usernamePassword.getPassword()));
        assertThrows(UsernamePasswordNotFoundException.class, () -> params.getUsernamePasswordForUrl("bleh"));
    }

}
