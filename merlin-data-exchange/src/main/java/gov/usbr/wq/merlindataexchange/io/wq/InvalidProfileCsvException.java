package gov.usbr.wq.merlindataexchange.io.wq;

import java.nio.file.Path;

final class InvalidProfileCsvException extends Exception
{
    InvalidProfileCsvException(Path csvFile, String error)
    {
        super(csvFile.toString() + " is invalid: " + error);
    }
}
