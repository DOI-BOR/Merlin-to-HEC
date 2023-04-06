package gov.usbr.wq.merlindataexchange.io;

import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.annotation.JsonUnwrapped;
import gov.usbr.wq.dataaccess.model.MeasureWrapper;
import gov.usbr.wq.dataaccess.model.TemplateWrapper;

@JsonPropertyOrder({ "templateWrapper", "measureWrapper" })
public class FlattenedTemplateMeasure {
    @JsonUnwrapped
    TemplateWrapper templateWrapper;
    @JsonUnwrapped
    MeasureWrapper measureWrapper;

    public FlattenedTemplateMeasure(TemplateWrapper templateWrapper, MeasureWrapper measureWrapper) {
        this.templateWrapper = templateWrapper;
        this.measureWrapper = measureWrapper;
    }


}
