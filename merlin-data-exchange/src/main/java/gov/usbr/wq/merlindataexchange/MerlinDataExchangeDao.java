package gov.usbr.wq.merlindataexchange;

import gov.usbr.wq.dataaccess.jwt.TokenContainer;
import gov.usbr.wq.dataaccess.model.MeasureWrapper;
import gov.usbr.wq.dataaccess.model.QualityVersionWrapper;
import hec.io.StoreOption;
import hec.ui.ProgressListener;
import rma.services.annotations.ServiceProvider;

import java.time.Instant;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;

@ServiceProvider(service = DataExchangeDao.class, position = 100, path = DataExchangeDao.LOOKUP_PATH
        + "/" + MerlinDataExchangeDao.MERLIN)
public final class MerlinDataExchangeDao implements DataExchangeDao
{
    public static final String MERLIN = "merlin";
    @Override
    public CompletableFuture<Void> exchangeData(MerlinDataExchangeParameters runtimeParameters, MeasureWrapper measure, QualityVersionWrapper qualityVersion,
                                                TokenContainer accessToken, String unitSystemToConvertTo, MerlinDaoWriter writer, MerlinExchangeDaoCompletionTracker completionTracker,
                                                ProgressListener progressListener, AtomicBoolean isCancelled, ExecutorService executorService)
    {
       CompletableFuture<Void> retVal = new CompletableFuture<>();
       if(!isCancelled.get())
       {
           MerlinDaoReader reader = new MerlinDaoReader();
           Instant startTime = runtimeParameters.getStart();
           Instant endTime = runtimeParameters.getEnd();
           String fPartOverride = runtimeParameters.getFPartOverride();
           StoreOption storeOption = runtimeParameters.getStoreOption();
           Integer qualityVersionId = qualityVersion == null ? null : qualityVersion.getQualityVersionID();
           retVal = reader.readData(startTime, endTime, measure, qualityVersionId, accessToken, unitSystemToConvertTo, fPartOverride, completionTracker, progressListener, isCancelled, executorService)
                   .thenAcceptAsync(tsc -> writer.writeData(tsc, storeOption, completionTracker, progressListener, isCancelled), executorService);
       }
       return retVal;
    }
}
