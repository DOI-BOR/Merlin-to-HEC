package gov.usbr.wq.merlindataexchange;

import com.rma.io.DssFileManager;
import com.rma.io.DssFileManagerImpl;
import gov.usbr.wq.dataaccess.MerlinTimeSeriesDataAccess;
import gov.usbr.wq.dataaccess.http.HttpAccessException;
import gov.usbr.wq.dataaccess.http.HttpAccessUtils;
import gov.usbr.wq.dataaccess.jwt.TokenContainer;
import gov.usbr.wq.dataaccess.model.MeasureWrapper;
import gov.usbr.wq.dataaccess.model.QualityVersionWrapper;
import gov.usbr.wq.dataaccess.model.TemplateWrapper;
import hec.ui.ProgressListener;
import hec.ui.ProgressListener.MessageType;
import rma.services.annotations.ServiceProvider;
import rma.util.lookup.Lookup;
import rma.util.lookup.Lookups;

import javax.xml.stream.XMLStreamException;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

@ServiceProvider(service = DataExchangeEngine.class, position = 100, path = DataExchangeEngine.LOOKUP_PATH
        + "/" + MerlinDataExchangeEngine.MERLIN)
public final class MerlinDataExchangeEngine implements DataExchangeEngine
{
    public static final String MERLIN = "merlin";
    private static final Logger LOGGER = Logger.getLogger(MerlinDataExchangeEngine.class.getName());
    private static final int PERCENT_COMPLETE_ALLOCATED_FOR_INITIAL_SETUP = 5;
    private static final int THREAD_COUNT = 5;
    private static final String THREAD_PROPERTY_KEY = "merlin.dataexchange.threadpool.size";
    private final ExecutorService _executorService = Executors.newFixedThreadPool(getThreadPoolSize(), new MerlinThreadFactory());
    private final List<TemplateWrapper> _cachedTemplates = new ArrayList<>();
    private final List<QualityVersionWrapper> _cachedQualityVersions = new ArrayList<>();
    private final Map<TemplateWrapper, List<MeasureWrapper>> _cachedTemplateToMeasurements = new HashMap<>();
    private final MerlinTimeSeriesDataAccess _merlinDataAccess = new MerlinTimeSeriesDataAccess();
    private final AtomicBoolean _isCancelled = new AtomicBoolean(false);
    private ProgressListener _progressListener;

    private static int getThreadPoolSize()
    {
        int retVal;
        //availableProcessors gives us logical cores, which includes hyperthreading stuff.  We can't determine if hyperthreading is on, so let's always halve the available processors.
        //Let's make sure we don't go lower than 1 by using Math.max.  1 / 2 = 0 in integer values, so this could be bad...
        String threadPoolSize = System.getProperty(THREAD_PROPERTY_KEY);
        if (threadPoolSize != null)
        {
            retVal = Math.max(Integer.parseInt(threadPoolSize), 1);
            LOGGER.log(Level.FINE, () -> "Merlin executor service created using System Property " + THREAD_PROPERTY_KEY + " with thread pool size of: " + retVal);
        }
        else
        {
            int coreCount = Math.max(getCoreCount(), 1);
            retVal = THREAD_COUNT * coreCount; //5 should cover bases for concurrent merlin web data retrieval?
            LOGGER.log(Level.FINE, () -> "System Property " + THREAD_PROPERTY_KEY + " not set. Merlin executor service created using default thread pool size of: " + retVal);
        }
        return retVal;
    }

    private static int getCoreCount()
    {
        return Runtime.getRuntime().availableProcessors() / 2;
    }

    @Override
    public CompletableFuture<Void> runExtract(List<Path> xmlConfigurationFiles, MerlinDataExchangeParameters runtimeParameters, ProgressListener progressListener) throws HttpAccessException
    {
        _progressListener = Objects.requireNonNull(progressListener, "Missing required ProgressListener");
        _isCancelled.set(false);
        _progressListener.start();
        TokenContainer token = HttpAccessUtils.authenticate(runtimeParameters.getUsername(), runtimeParameters.getPassword());
        List<DataExchangeConfiguration> parsedConfigurations = parseConfigurations(xmlConfigurationFiles);
        return initializeCacheAsync(parsedConfigurations, token)
                .whenComplete((idc, e) -> beginExtract(parsedConfigurations, token, runtimeParameters, e));
    }

    @Override
    public void cancelExtract()
    {
        _isCancelled.set(true);
        if(_progressListener != null)
        {
            _progressListener.progress("Merlin Data Exchanged Cancelled. Finishing gracefully...", MessageType.IMPORTANT);
        }
    }

    private void beginExtract(List<DataExchangeConfiguration> xmlConfigurationFiles, TokenContainer token, MerlinDataExchangeParameters runtimeParameters, Throwable initializationError)
    {
        if(initializationError != null)
        {
            LOGGER.log(Level.WARNING, initializationError, () -> "Failed to initialize templates, quality versions, and measures cache");
        }
        else
        {
            if(!_isCancelled.get())
            {
                int totalMeasures = _cachedTemplateToMeasurements.values().stream()
                        .mapToInt(List::size)
                        .sum();
                MerlinExchangeDaoCompletionTracker completionTracker = new MerlinExchangeDaoCompletionTracker(totalMeasures, PERCENT_COMPLETE_ALLOCATED_FOR_INITIAL_SETUP);
                xmlConfigurationFiles.forEach(xmlConfigurationFile -> extractConfiguration(xmlConfigurationFile, runtimeParameters, token, completionTracker));
            }
            finish();
        }
    }

    private void finish()
    {
        if(_isCancelled.get())
        {
            _progressListener.progress("Merlin Data Exchanged Cancelled successfully", MessageType.IMPORTANT);
        }
        _progressListener.finish();
    }

    private List<DataExchangeConfiguration> parseConfigurations(List<Path> xmlConfigurationFiles)
    {
        List<DataExchangeConfiguration> retVal = new ArrayList<>();
        xmlConfigurationFiles.forEach(configFilepath -> retVal.add(parseDataExchangeConfiguration(configFilepath)));
        _progressListener.progress("Configuration files parsed", MessageType.IMPORTANT, 1);
        return retVal;
    }

    private void extractConfiguration(DataExchangeConfiguration dataExchangeConfig, MerlinDataExchangeParameters runtimeParameters, TokenContainer token,
                                      MerlinExchangeDaoCompletionTracker completionTracker)
    {
        if (dataExchangeConfig != null)
        {
            List<DataExchangeSet> dataExchangeSets = dataExchangeConfig.getDataExchangeSets();
            dataExchangeSets.forEach(dataExchangeSet ->
            {
                if(!_isCancelled.get())
                {
                    exchangeDataForSet(dataExchangeSet, dataExchangeConfig, runtimeParameters, token, completionTracker);
                }
            });
        }
    }

    private void exchangeDataForSet(DataExchangeSet dataExchangeSet, DataExchangeConfiguration dataExchangeConfig,
                                    MerlinDataExchangeParameters runtimeParameters, TokenContainer token, MerlinExchangeDaoCompletionTracker completionTracker)
    {
        DataStoreRef dataStoreRefB = dataExchangeSet.getDataStoreRefB();
        DataStoreRef destinationRef = dataStoreRefB;
        if (dataExchangeSet.getSourceId().equalsIgnoreCase(dataStoreRefB.getId()))
        {
            destinationRef = dataExchangeSet.getDataStoreRefA();
        }
        Optional<DataStore> dataStoreDestinationOpt = dataExchangeConfig.getDataStoreByRef(destinationRef);
        if(dataStoreDestinationOpt.isPresent())
        {
            exchangeData(dataExchangeSet, dataStoreDestinationOpt.get(), runtimeParameters, token, completionTracker);
        }
        else
        {
            _progressListener.progress("No datastore found for datastore-ref id: " + destinationRef.getId(), MessageType.ERROR);
        }
    }

    private void exchangeData(DataExchangeSet dataExchangeSet, DataStore dataStoreDestination,
                              MerlinDataExchangeParameters runtimeParameters, TokenContainer token, MerlinExchangeDaoCompletionTracker completionTracker)
    {
        TemplateWrapper template = getTemplateFromDataExchangeSet(dataExchangeSet, _progressListener);
        if(template != null)
        {
            Optional<QualityVersionWrapper> qualityVersionOpt = getQualityVersionIdFromDataExchangeSet(dataExchangeSet, _progressListener);
            String unitSystemToConvertTo = dataExchangeSet.getUnitSystem();
            _progressListener.progress("Specified unit system: " + unitSystemToConvertTo, MessageType.IMPORTANT);
            Path absolutePathToWriteTo = buildAbsoluteDssWritePath(dataStoreDestination.getPath(), runtimeParameters.getWatershedDirectory());
            _progressListener.progress("Path to write to: " + absolutePathToWriteTo);
            DataExchangeDao dao = lookUpDao();
            List<MeasureWrapper> measures = _cachedTemplateToMeasurements.get(template);
            List<CompletableFuture<Void>> measurementFutures = new ArrayList<>();
            DssFileManager dssFileManager = DssFileManagerImpl.getDssFileManager();
            MerlinDaoWriter writer = new MerlinDaoWriter(dssFileManager, absolutePathToWriteTo);
            try
            {
                measures.forEach(measure ->
                        measurementFutures.add(dao.exchangeData(runtimeParameters,
                                measure, qualityVersionOpt.orElse(null), token, unitSystemToConvertTo, writer, completionTracker, _progressListener,
                                _isCancelled, _executorService)));
                CompletableFuture.allOf(measurementFutures.toArray(new CompletableFuture[0])).join();
            }
            finally
            {
                writer.close();
            }

        }
    }

    private Path buildAbsoluteDssWritePath(String filepath, Path watershedDir)
    {
        Path xmlFilePath = Paths.get(filepath);
        if(!xmlFilePath.isAbsolute() && filepath.contains("$WATERSHED"))
        {
            filepath = filepath.replace("$WATERSHED", watershedDir.toString());
            xmlFilePath = Paths.get(filepath);
        }
        return xmlFilePath;
    }

    private DataExchangeDao lookUpDao()
    {
        String delimeter = "/";
        String lookupPath = DataExchangeDao.LOOKUP_PATH + delimeter + MerlinDataExchangeDao.MERLIN;
        LOGGER.log(Level.FINE, () -> "Looking for Dao at: " + lookupPath);
        Lookup lookup = Lookups.forPath(lookupPath);
        return lookup.lookup(DataExchangeDao.class);
    }

    private List<MeasureWrapper> retrieveMeasures(TokenContainer accessToken, TemplateWrapper template, ProgressListener progressListener)
    {
        List<MeasureWrapper> retVal = new ArrayList<>();
        try
        {
            progressListener.progress("Retrieving measures for template " + template.getName() + " (id: " + template.getDprId() + ")...");
            retVal = _merlinDataAccess.getMeasurementsByTemplate(accessToken, template);
            progressListener.progress("Successfully retrieved " + retVal.size() + " measures!", MessageType.IMPORTANT, PERCENT_COMPLETE_ALLOCATED_FOR_INITIAL_SETUP);
        }
        catch (HttpAccessException | IOException ex)
        {
            LOGGER.log(Level.WARNING, ex, () -> "Unable to access the merlin web services to retrieve measures for template " + template);
            progressListener.progress("Failed to retrieve measures for template " + template.getName() + " (id: " + template.getDprId() + ")", MessageType.ERROR);
        }
        return retVal;
    }

    private Optional<QualityVersionWrapper> getQualityVersionIdFromDataExchangeSet(DataExchangeSet dataExchangeSet, ProgressListener progressListener)
    {
        String qualityVersionNameFromSet = dataExchangeSet.getQualityVersionName();
        Integer qualityVersionIdFromSet = dataExchangeSet.getQualityVersionId();
        progressListener.progress("Retrieving Quality Version for " + qualityVersionNameFromSet + " (id: " +  qualityVersionIdFromSet + ")", MessageType.IMPORTANT);
        Optional<QualityVersionWrapper> retVal = _cachedQualityVersions.stream()
                .filter(qualityVersion -> qualityVersion.getQualityVersionName().equalsIgnoreCase(qualityVersionNameFromSet))
                .findFirst();
        if(!retVal.isPresent())
        {
            retVal = _cachedQualityVersions.stream()
                    .filter(qualityVersion -> qualityVersion.getQualityVersionID().intValue() == qualityVersionIdFromSet)
                    .findFirst();
        }
        if(!retVal.isPresent())
        {
            LOGGER.log(Level.WARNING, () -> "Failed to find matching quality version ID in retrieved quality versions for quality version name "
                    + qualityVersionNameFromSet + " or id " + qualityVersionIdFromSet
                    + ". Using NULL for quality version.");
        }
        return retVal;
    }

    private TemplateWrapper getTemplateFromDataExchangeSet(DataExchangeSet dataExchangeSet, ProgressListener progressListener)
    {
        String templateNameFromSet = dataExchangeSet.getTemplateName();
        int templateIdFromSet = dataExchangeSet.getTemplateId();
        if(progressListener != null)
        {
            progressListener.progress("Retrieving Template for " + templateNameFromSet + " (id: " + templateIdFromSet + ")", MessageType.IMPORTANT);
        }
        TemplateWrapper retVal = _cachedTemplates.stream()
                .filter(template -> template.getName().equalsIgnoreCase(templateNameFromSet))
                .findFirst()
                .orElse(null);
        if(retVal == null)
        {
            retVal = _cachedTemplates.stream()
                    .filter(template -> template.getDprId() == templateIdFromSet)
                    .findFirst()
                    .orElse(null);
        }
        if(retVal == null)
        {
            String errorMsg = "Failed to find matching template ID in retrieved templates for template name " + templateNameFromSet + " or id " + templateIdFromSet;
            LOGGER.log(Level.WARNING, () -> errorMsg);
            if(progressListener != null)
            {
                progressListener.progress(errorMsg, MessageType.ERROR);
            }
        }
        return retVal;
    }

    private DataExchangeConfiguration parseDataExchangeConfiguration(Path xmlConfigurationFile)
    {
        DataExchangeConfiguration retVal = null;
        try
        {
            _progressListener.progress("Parsing configuration file: " + xmlConfigurationFile.toString() + "...", MessageType.IMPORTANT);
            retVal = MerlinDataExchangeParser.parseXmlFile(xmlConfigurationFile);
            _progressListener.progress("Parsed configuration file successfully!", MessageType.IMPORTANT);
        }
        catch (IOException | XMLStreamException e)
        {
            String errorMsg = "Failed to parse data exchange configuration xml for " + xmlConfigurationFile.toString();
            LOGGER.log(Level.WARNING, e, () -> errorMsg);
            _progressListener.progress(errorMsg, MessageType.ERROR);
        }
        return retVal;
    }

    private CompletableFuture<Void> initializeCacheAsync(List<DataExchangeConfiguration> parsedConfigurations, TokenContainer token)
    {
        return CompletableFuture.runAsync(() ->
        {
            if(_cachedTemplates.isEmpty() || _cachedQualityVersions.isEmpty())
            {
                try
                {
                    _progressListener.progress("Retrieving templates from Merlin Web Service", MessageType.IMPORTANT);
                    _cachedTemplates.addAll(_merlinDataAccess.getTemplates(token));
                    _progressListener.progress("Successfully retrieved " + _cachedTemplates.size() + " templates.", MessageType.IMPORTANT, (int) (PERCENT_COMPLETE_ALLOCATED_FOR_INITIAL_SETUP * 0.4));
                    _progressListener.progress("Retrieving quality versions from Merlin Web Service", MessageType.IMPORTANT);
                    _cachedQualityVersions.addAll(_merlinDataAccess.getQualityVersions(token));
                    _progressListener.progress("Successfully retrieved " + _cachedQualityVersions.size() + " quality versions.", MessageType.IMPORTANT,
                            (int) (PERCENT_COMPLETE_ALLOCATED_FOR_INITIAL_SETUP * 0.6));
                    if(!_isCancelled.get())
                    {
                        initializeCachedMeasurements(parsedConfigurations, token);
                    }
                }
                catch (IOException | HttpAccessException e)
                {
                    _progressListener.progress("Failed to retrieve templates and quality versions from Merlin Web Service", MessageType.ERROR);
                    throw new CompletionException(e);
                }
            }
            else
            {
                initializeCachedMeasurements(parsedConfigurations, token);
            }
        }, _executorService);
    }

    private void initializeCachedMeasurements(List<DataExchangeConfiguration> parsedConfigurations, TokenContainer token)
    {
        parsedConfigurations.forEach(dataExchangeConfig ->
        {
            List<DataExchangeSet> exchangeSets = dataExchangeConfig.getDataExchangeSets();
            exchangeSets.forEach(set ->
            {
                TemplateWrapper template = getTemplateFromDataExchangeSet(set, _progressListener);
                if(template != null && !_cachedTemplateToMeasurements.containsKey(template))
                {
                    List<MeasureWrapper> measures = retrieveMeasures(token, template, _progressListener);
                    _cachedTemplateToMeasurements.put(template, measures);
                }
            });
        });
    }

}
