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
    private int _templateId;
    @JacksonXmlProperty(isAttribute = true, localName = "template-name")
    private String _templateName;
    @JacksonXmlProperty(isAttribute = true, localName = "quality-version-id")
    private Integer _qualityVersionId;
    @JacksonXmlProperty(isAttribute = true, localName = "quality-version-name")
    private String _qualityVersionName;
    @JacksonXmlProperty(isAttribute = true, localName = "unit-system")
    private String _unitSystem = Unit.SI;
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

    public void setId(String id)
    {
        _id = id;
    }

    public String getSourceId()
    {
        return _sourceId;
    }

    public void setSourceId(String sourceId)
    {
        _sourceId = sourceId;
    }

    public int getTemplateId()
    {
        return _templateId;
    }

    public void setTemplateId(int templateId)
    {
        _templateId = templateId;
    }

    public Integer getQualityVersionId()
    {
        return _qualityVersionId;
    }

    public void setQualityVersionId(Integer qualityVersionId)
    {
        _qualityVersionId = qualityVersionId;
    }

    public String getUnitSystem()
    {
        return _unitSystem;
    }

    public void setUnitSystem(String unitSystem)
    {
        _unitSystem = unitSystem;
    }

    public DataStoreRef getDataStoreRefA()
    {
        return _dataStoreRefA;
    }

    public void setDataStoreRefA(DataStoreRef dataStoreRefA)
    {
        _dataStoreRefA = dataStoreRefA;
    }

    public DataStoreRef getDataStoreRefB()
    {
        return _dataStoreRefB;
    }

    public void setDataStoreRefB(DataStoreRef dataStoreRefB)
    {
        _dataStoreRefB = dataStoreRefB;
    }

    public String getTemplateName()
    {
        return _templateName;
    }

    public void setTemplateName(String templateName)
    {
        _templateName = templateName;
    }

    public String getQualityVersionName()
    {
        return _qualityVersionName;
    }

    public void setQualityVersionName(String qualityVersionName)
    {
        _qualityVersionName = qualityVersionName;
    }

    public String getDataType()
    {
        return _dataType;
    }

    public void setDataType(String dataType)
    {
        _dataType = dataType;
    }
}
