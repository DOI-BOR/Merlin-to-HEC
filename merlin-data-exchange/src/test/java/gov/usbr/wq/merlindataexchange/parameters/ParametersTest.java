package gov.usbr.wq.merlindataexchange.parameters;

import hec.io.impl.StoreOptionImpl;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.ArrayList;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

final class ParametersTest
{
    @Test
    void testBuildAuthenticationParameters()
    {
		// This username and password are not valid and will fail unless replaced by the user
        AuthenticationParameters authParams = new AuthenticationParametersBuilder()
                .forUrl("https://www.grabdata2.com")
                .setUsername("user")
                .andPassword("password".toCharArray())
                .build();
        assertEquals("https://www.grabdata2.com", authParams.getUrl());
        assertEquals("user", authParams.getUsernamePassword().getUsername());
        assertEquals("password", new String(authParams.getUsernamePassword().getPassword()));

        assertThrows(NullPointerException.class, () ->  new AuthenticationParametersBuilder()
                .forUrl(null)
                .setUsername("user")
                .andPassword("password".toCharArray())
                .build());
        assertThrows(NullPointerException.class, () ->  new AuthenticationParametersBuilder()
                .forUrl("https://www.grabdata2.com")
                .setUsername(null)
                .andPassword("password".toCharArray())
                .build());
        assertThrows(NullPointerException.class, () ->  new AuthenticationParametersBuilder()
                .forUrl("https://www.grabdata2.com")
                .setUsername("user")
                .andPassword(null)
                .build());
    }

    @Test
    void testBuildMerlinTimeSeriesParameters() throws UsernamePasswordNotFoundException {
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
        MerlinTimeSeriesParameters params = new MerlinTimeSeriesParametersBuilder()
                .withWatershedDirectory(workingDir)
                .withLogFileDirectory(logDir)
                .withAuthenticationParameters(authParams)
                .withStoreOption(storeOption)
                .withStart(start)
                .withEnd(end)
                .withFPartOverride("fPart")
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
    void testBuildMerlinProfileParameters() throws UsernamePasswordNotFoundException {
        Path workingDir = Paths.get(System.getProperty("user.dir"));
        Instant start = Instant.parse("2019-01-01T08:00:00Z");
        Instant end = Instant.parse("2022-08-30T08:00:00Z");
        AuthenticationParameters authParams = new AuthenticationParametersBuilder()
                .forUrl("https://www.grabdata2.com")
                .setUsername("user")
                .andPassword("password".toCharArray())
                .build();
        Path logDir = workingDir.resolve("log");
        MerlinProfileParameters params = new MerlinProfileParametersBuilder()
                .withWatershedDirectory(workingDir)
                .withLogFileDirectory(logDir)
                .withAuthenticationParameters(authParams)
                .withStart(start)
                .withEnd(end)
                .build();
        assertEquals(params.getWatershedDirectory(), workingDir);
        assertEquals(params.getLogFileDirectory(), logDir);
        assertEquals(params.getStart(), start);
        assertEquals(params.getEnd(), end);
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
        MerlinTimeSeriesParameters params = new MerlinTimeSeriesParametersBuilder()
                .withWatershedDirectory(workingDir)
                .withLogFileDirectory(workingDir)
                .withAuthenticationParameters(authParams)
                .withStoreOption(storeOption)
                .withStart(start)
                .withEnd(end)
                .withFPartOverride("fPart")
                .build();
        UsernamePasswordHolder usernamePassword = params.getUsernamePasswordForUrl("https://www.grabdata2.com");
        assertEquals("user", usernamePassword.getUsername());
        assertEquals("password", new String(usernamePassword.getPassword()));
        assertThrows(UsernamePasswordNotFoundException.class, () -> params.getUsernamePasswordForUrl("bleh"));
    }

    @Test
    void testNulls()
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
        Path logDir = workingDir.resolve("log");
        assertThrows(NullPointerException.class, () -> new MerlinTimeSeriesParametersBuilder()
                .withWatershedDirectory(null)
                .withLogFileDirectory(logDir)
                .withAuthenticationParameters(authParams)
                .withStoreOption(storeOption)
                .withStart(start)
                .withEnd(end)
                .withFPartOverride("fPart")
                .build());
        assertThrows(NullPointerException.class, () -> new MerlinTimeSeriesParametersBuilder()
                .withWatershedDirectory(workingDir)
                .withLogFileDirectory(null)
                .withAuthenticationParameters(authParams)
                .withStoreOption(storeOption)
                .withStart(start)
                .withEnd(end)
                .withFPartOverride("fPart")
                .build());
        assertThrows(IllegalArgumentException.class, () -> new MerlinTimeSeriesParametersBuilder()
                .withWatershedDirectory(workingDir)
                .withLogFileDirectory(logDir)
                .withAuthenticationParameters(null)
                .withStoreOption(storeOption)
                .withStart(start)
                .withEnd(end)
                .withFPartOverride("fPart")
                .build());
        assertThrows(IllegalArgumentException.class, () -> new MerlinTimeSeriesParametersBuilder()
                .withWatershedDirectory(workingDir)
                .withLogFileDirectory(logDir)
                .withAuthenticationParametersList(null)
                .withStoreOption(storeOption)
                .withStart(start)
                .withEnd(end)
                .withFPartOverride("fPart")
                .build());
        assertThrows(IllegalArgumentException.class, () -> new MerlinTimeSeriesParametersBuilder()
                .withWatershedDirectory(workingDir)
                .withLogFileDirectory(logDir)
                .withAuthenticationParametersList(new ArrayList<>())
                .withStoreOption(storeOption)
                .withStart(start)
                .withEnd(end)
                .withFPartOverride("fPart")
                .build());
        assertThrows(NullPointerException.class, () -> new MerlinTimeSeriesParametersBuilder()
                .withWatershedDirectory(workingDir)
                .withLogFileDirectory(logDir)
                .withAuthenticationParameters(authParams)
                .withStoreOption(null)
                .withStart(start)
                .withEnd(end)
                .withFPartOverride("fPart")
                .build());
    }

    @Test
    void testBuildFromMerlinTimeSeriesParameters() throws UsernamePasswordNotFoundException
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
        Path logDir = workingDir.resolve("log");
        MerlinTimeSeriesParameters params = new MerlinTimeSeriesParametersBuilder()
                .withWatershedDirectory(workingDir)
                .withLogFileDirectory(logDir)
                .withAuthenticationParameters(authParams)
                .withStoreOption(storeOption)
                .withStart(start)
                .withEnd(end)
                .withFPartOverride("fPart")
                .build();
        params = new MerlinTimeSeriesParametersBuilder()
                .fromExistingParameters(params)
                .withAuthenticationParameters(new AuthenticationParametersBuilder()
                        .forUrl("https://www.grabdata2.com")
                        .setUsername("userNew")
                        .andPassword("passwordNew".toCharArray())
                        .build())
                .build();
        assertEquals(params.getWatershedDirectory(), workingDir);
        assertEquals(params.getLogFileDirectory(), logDir);
        assertEquals(params.getStart(), start);
        assertEquals(params.getEnd(), end);
        assertEquals(params.getStoreOption(), storeOption);
        assertEquals(params.getFPartOverride(), "fPart");
        UsernamePasswordHolder usernamePassword = params.getUsernamePasswordForUrl("https://www.grabdata2.com");
        assertEquals("userNew", usernamePassword.getUsername());
        assertEquals("passwordNew", new String(usernamePassword.getPassword()));

        params = new MerlinTimeSeriesParametersBuilder()
                .fromExistingParameters(params)
                .build();
        assertEquals(params.getWatershedDirectory(), workingDir);
        assertEquals(params.getLogFileDirectory(), logDir);
        assertEquals(params.getStart(), start);
        assertEquals(params.getEnd(), end);
        assertEquals(params.getStoreOption(), storeOption);
        assertEquals(params.getFPartOverride(), "fPart");
        usernamePassword = params.getUsernamePasswordForUrl("https://www.grabdata2.com");
        assertEquals("userNew", usernamePassword.getUsername());
        assertEquals("passwordNew", new String(usernamePassword.getPassword()));

        storeOption.setRegular("1-replace-missing-values-only");
        params = new MerlinTimeSeriesParametersBuilder()
                .fromExistingParameters(params)
                .withWatershedDirectory(workingDir.resolve("changed"))
                .withLogFileDirectory(workingDir.resolve("changedLog"))
                .withStart(Instant.parse("2020-01-01T08:00:00Z"))
                .withEnd(Instant.parse("2021-01-01T08:00:00Z"))
                .withFPartOverride("fPartChanged")
                .withStoreOption(storeOption)
                .build();
        assertEquals(params.getWatershedDirectory(), workingDir.resolve("changed"));
        assertEquals(params.getLogFileDirectory(), workingDir.resolve("changedLog"));
        assertEquals(params.getStart(), Instant.parse("2020-01-01T08:00:00Z"));
        assertEquals(params.getEnd(), Instant.parse("2021-01-01T08:00:00Z"));
        assertEquals(params.getStoreOption(), storeOption);
        assertEquals(params.getFPartOverride(), "fPartChanged");
        usernamePassword = params.getUsernamePasswordForUrl("https://www.grabdata2.com");
        assertEquals("userNew", usernamePassword.getUsername());
        assertEquals("passwordNew", new String(usernamePassword.getPassword()));
    }


}
