package gov.usbr.wq.merlindataexchange;

final class UnsupportedQualityVersionException extends Exception
{
    UnsupportedQualityVersionException(String qualityVersionName, Integer qualityVersionId)
    {
        super("Failed to find matching quality version for quality version name "
                + qualityVersionName + " or id " + qualityVersionId);
    }
}
