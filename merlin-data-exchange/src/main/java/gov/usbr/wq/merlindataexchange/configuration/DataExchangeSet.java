package gov.usbr.wq.merlindataexchange.configuration;

import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlProperty;
import hec.heclib.util.Unit;

public final class DataExchangeSet
{
    @JacksonXmlProperty(isAttribute = true, localName = "id")
    private String _id;
    @JacksonXmlProperty(isAttribute = true, localName = "source-id")
    private String _sourceId;
    @JacksonXmlProperty(isAttribute = true, localName = "template-id")
    private Integer _templateId;
    @JacksonXmlProperty(isAttribute = true, localName = "template-name")
    private String _templateName;
    @JacksonXmlProperty(isAttribute = true, localName = "quality-version-id")
    private Integer _qualityVersionId;
    @JacksonXmlProperty(isAttribute = true, localName = "quality-version-name")
    private String _qualityVersionName;
    @JacksonXmlProperty(isAttribute = true, localName = "unit-system")
    private String _unitSystem = Unit.ENGLISH;
    @JacksonXmlProperty(localName = "data-type")
    private String _dataType;

    @JacksonXmlProperty(localName = "datastore-ref-a")
    private DataStoreRef _dataStoreRefA;
    @JacksonXmlProperty(localName = "datastore-ref-b")
    private DataStoreRef _dataStoreRefB;

    public  String getId()
    {
        return _id;
    }

    public String getSourceId()
    {
        return _sourceId;
    }

    public Integer getTemplateId()
    {
        return _templateId;
    }

    public Integer getQualityVersionId()
    {
        return _qualityVersionId;
    }

    public String getUnitSystem()
    {
        return _unitSystem;
    }

    public DataStoreRef getDataStoreRefA()
    {
        return _dataStoreRefA;
    }

    public DataStoreRef getDataStoreRefB()
    {
        return _dataStoreRefB;
    }

    public String getTemplateName()
    {
        return _templateName;
    }

    public String getQualityVersionName()
    {
        return _qualityVersionName;
    }

    public String getDataType()
    {
        return _dataType;
    }
}
