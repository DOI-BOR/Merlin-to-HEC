package gov.usbr.wq.merlindataexchange.integration;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import gov.usbr.wq.merlindataexchange.DataExportEngine;
import gov.usbr.wq.merlindataexchange.MerlinDataExchangeStatus;
import gov.usbr.wq.merlindataexchange.MerlinDataExportEngineFluentBuilder;
import gov.usbr.wq.merlindataexchange.ResourceAccess;
import gov.usbr.wq.merlindataexchange.fluentbuilders.ExportType;
import gov.usbr.wq.merlindataexchange.fluentbuilders.FluentAuthenticationBuilder;
import gov.usbr.wq.merlindataexchange.parameters.AuthenticationParametersBuilder;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class MerlinDataExportIntegrationTest
{

    private Path getTestDirectory()
    {
        return Paths.get(System.getProperty("user.dir")).resolve("build/tmp");
    }

    @Test
    void testExportCsv() throws IOException
    {
        String username = ResourceAccess.getUsername();
        char[] password = ResourceAccess.getPassword();
        String csvFileName = "merlin_template_measure.csv";
        String csvPath = getTestDirectory().resolve(csvFileName).toString();

        FluentAuthenticationBuilder authenticationBuilder = new AuthenticationParametersBuilder()
                .forUrl("https://www.grabdata2.com")
                .setUsername(username)
                .andPassword(password);

        DataExportEngine dataExport = new MerlinDataExportEngineFluentBuilder()
                .withAuthenticationParameters(authenticationBuilder.build())
                .withExportFilePath(csvPath)
                .withExportType(ExportType.CSV)
                .build();

        MerlinDataExchangeStatus status = dataExport.runExport().join();
        assertEquals(MerlinDataExchangeStatus.COMPLETE_SUCCESS, status);
        assertTrue(Files.size(Paths.get(csvPath)) > 0);
    }

    @Test
    void testExportXlsx() throws IOException
    {
        String username = ResourceAccess.getUsername();
        char[] password = ResourceAccess.getPassword();
        String xlsxFileName = "merlin_template_measure.xlsx";
        String xlsxPath = getTestDirectory().resolve(xlsxFileName).toString();

        FluentAuthenticationBuilder authenticationBuilder = new AuthenticationParametersBuilder()
                .forUrl("https://www.grabdata2.com")
                .setUsername(username)
                .andPassword(password);

        DataExportEngine dataExport = new MerlinDataExportEngineFluentBuilder()
                .withAuthenticationParameters(authenticationBuilder.build())
                .withExportFilePath(xlsxPath)
                .withExportType(ExportType.XLSX)
                .build();

        MerlinDataExchangeStatus status = dataExport.runExport().join();
        assertEquals(MerlinDataExchangeStatus.COMPLETE_SUCCESS, status);
        assertTrue(Files.size(Paths.get(xlsxPath)) > 0);

    }


}