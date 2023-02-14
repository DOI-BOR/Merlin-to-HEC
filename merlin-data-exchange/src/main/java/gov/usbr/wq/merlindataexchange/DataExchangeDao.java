package gov.usbr.wq.merlindataexchange;

import gov.usbr.wq.dataaccess.jwt.TokenContainer;
import gov.usbr.wq.dataaccess.model.MeasureWrapper;
import gov.usbr.wq.dataaccess.model.QualityVersionWrapper;
import hec.ui.ProgressListener;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;

public interface DataExchangeDao
{
    String LOOKUP_PATH = "dataexchange/dao";

    CompletableFuture<Void> exchangeData(MerlinDataExchangeParameters runtimeParameters, MeasureWrapper measure, QualityVersionWrapper qualityVersionId,
                                         TokenContainer accessToken, String unitSystemToConvertTo, MerlinDaoWriter writer, MerlinExchangeDaoCompletionTracker extractedMeasureCount,
                                         ProgressListener progressListener, AtomicBoolean isCancelled, ExecutorService executorService);

}
