package gov.usbr.wq.merlindataexchange;

import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlProperty;
import hec.heclib.util.Unit;

final class DataExchangeSet
{
    @JacksonXmlProperty(isAttribute = true, localName = "id")
    private String _id;
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


    String getId()
    {
        return _id;
    }

    void setId(String id)
    {
        _id = id;
    }

    int getTemplateId()
    {
        return _templateId;
    }

    void setTemplateId(int templateId)
    {
        _templateId = templateId;
    }

    Integer getQualityVersionId()
    {
        return _qualityVersionId;
    }

    void setQualityVersionId(Integer qualityVersionId)
    {
        _qualityVersionId = qualityVersionId;
    }

    String getUnitSystem()
    {
        return _unitSystem;
    }

    void setUnitSystem(String unitSystem)
    {
        _unitSystem = unitSystem;
    }

    DataStoreRef getDataStoreRefA()
    {
        return _dataStoreRefA;
    }

    void setDataStoreRefA(DataStoreRef dataStoreRefA)
    {
        _dataStoreRefA = dataStoreRefA;
    }

    DataStoreRef getDataStoreRefB()
    {
        return _dataStoreRefB;
    }

    void setDataStoreRefB(DataStoreRef dataStoreRefB)
    {
        _dataStoreRefB = dataStoreRefB;
    }

    String getTemplateName()
    {
        return _templateName;
    }

    void setTemplateName(String templateName)
    {
        _templateName = templateName;
    }

    String getQualityVersionName()
    {
        return _qualityVersionName;
    }

    void setQualityVersionName(String qualityVersionName)
    {
        _qualityVersionName = qualityVersionName;
    }

    String getDataType()
    {
        return _dataType;
    }

    void setDataType(String dataType)
    {
        _dataType = dataType;
    }
}
