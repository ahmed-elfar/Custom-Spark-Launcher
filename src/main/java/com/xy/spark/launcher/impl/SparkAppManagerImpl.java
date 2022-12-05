package com.xy.spark.launcher.impl;

import com.xy.spark.launcher.SparkAppState;
import com.xy.spark.launcher.StreamListener;
import com.xy.spark.launcher.proxy.SparkAppProxy;
import com.xy.spark.launcher.proxy.SparkAppProxyDefaultStreamReaders;
import com.xy.spark.launcher.AppConfig;
import com.xy.spark.launcher.SparkAppManager;
import com.xy.spark.launcher.proxy.SparkAppProxyStreamReaders;
import org.apache.spark.launcher.SparkAppHandle;
import org.apache.spark.launcher.SparkLauncher;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

import static com.xy.spark.launcher.impl.Util.checkNotNullNorEmpty;
import static com.xy.spark.launcher.impl.Util.isValidInput;

class SparkAppManagerImpl implements SparkAppManager {

    private static final int APP_MAP_INITIAL_CAPACITY = 50;

    private static SparkAppManagerImpl getInstance() {
        return LazyInstance.INSTANCE;
    }

    private static class LazyInstance {
        private static final SparkAppManagerImpl INSTANCE = new SparkAppManagerImpl();
    }

    private final Map<String, SparkAppProxy> sparkAppInfoMap;
    private SparkAppManagerImpl() {
        sparkAppInfoMap = new ConcurrentHashMap<>(APP_MAP_INITIAL_CAPACITY, 1);
    }

    @Override
    public Optional<SparkAppProxy> getSparkAppProxy(String appKey){
        SparkAppProxy sparkAppProxy = this.sparkAppInfoMap.get(appKey);
        if(sparkAppProxy == null){
            //TODO log warning
            System.err.println("No Spark Application found for key: " + appKey);
            return Optional.empty();
        }
        Optional<SparkAppProxy> optionalSparkAppProxy = Optional.of(sparkAppProxy);
        return optionalSparkAppProxy;
    }

    @Override
    public SparkAppProxy submitApp(String appKey, AppConfig appConfig) throws IOException {
        SparkAppProxy sparkAppProxy = createSparkAppProxy(appConfig, false);
        this.sparkAppInfoMap.put(appKey, sparkAppProxy);
        return sparkAppProxy;
    }

    @Override
    public SparkAppProxyStreamReaders submitAppWithStreamReaders(String appKey, AppConfig appConfig) throws IOException {
        SparkAppProxyStreamReaders sparkProxyWithStreams = createSparkAppProxyStreamReaders(appConfig);
        this.sparkAppInfoMap.put(appKey, sparkProxyWithStreams);
        return sparkProxyWithStreams;
    }

    @Override
    public SparkAppProxyDefaultStreamReaders submitAppWithDefaultStreamReaders(String appKey, AppConfig appConfig) throws IOException {
        List<String> output = new LinkedList<>();
        StreamListener outputListener = buildOutputStreamListener(output);
        appConfig.getOutputListeners().add(outputListener);

        StringBuilder errorLog = new StringBuilder();
        StreamListener errorListener = buildErrorStreamListener(errorLog);
        appConfig.getErrorListeners().add(errorListener);

        SparkAppProxyStreamReaders sparkProxyWithStreams = createSparkAppProxyStreamReaders(appConfig);
        SparkAppProxyDefaultStreamReaders sparkAppProxyDefaultStreamReaders = new SparkAppProxyDefaultStreamReadersImpl(sparkProxyWithStreams, output, errorLog);
        sparkAppInfoMap.put(appKey, sparkAppProxyDefaultStreamReaders);
        return sparkAppProxyDefaultStreamReaders;
    }

    @Override
    public void purge(String appKey){
        SparkAppProxy sparkAppProxy = this.sparkAppInfoMap.remove(appKey);
        if(sparkAppProxy == null){
            //TODO log warning
            System.err.println("Attempts to purge unknown Spark Application for key: " + appKey);
            return;
        }
        sparkAppProxy.stop();
        if(sparkAppProxy instanceof SparkAppProxyStreamReaders){
            ((SparkAppProxyStreamReaders)sparkAppProxy).stopStreamReaders();
        }
    }

    @Override
    public SparkAppState getAppState(String appKey) {
        SparkAppProxy sparkAppProxy = this.sparkAppInfoMap.get(appKey);
        if(sparkAppProxy != null){
            return sparkAppProxy.getState();
        }
        //TODO log warning
        System.err.println("No Spark Application found for key: " + appKey);
        return SparkAppState.NO_APPLICATION;
    }

    private SparkAppProxy createSparkAppProxy(AppConfig appConfig, boolean splitStreams) throws IOException {
        return createSparkAppProxy(appConfig.getAppName(), appConfig.getAppResource(), appConfig.getMainClass(),
                appConfig.getSparkHome(), appConfig.getSparkMaster(), appConfig.getConf(), appConfig.getAppArgs(), appConfig.getStateListener(),splitStreams);
    }

    private SparkAppProxy createSparkAppProxy(String appName, String appResource, String mainClass, String sparkHome, String sparkMaster, Map<String, String> conf, String[] appArgs, SparkAppState.Listener stateListener, boolean splitStream) throws IOException {
        SparkLauncher spark = createSparkLauncher(appName, appResource, mainClass, conf, appArgs, sparkHome, sparkMaster);
        if(splitStream){
            spark.splitStreams();
        }
        DefaultSparkListener defaultSparkListener = new DefaultSparkListener(stateListener);
        SparkAppHandle sparkAppHandle =  spark.startApplication(defaultSparkListener);
        SparkAppProxy sparkAppProxy = new SparkAppProxyImpl(defaultSparkListener, sparkAppHandle);
        return sparkAppProxy;
    }

    private SparkAppProxyStreamReaders createSparkAppProxyStreamReaders(AppConfig appConfig) throws IOException {
        if(appConfig.getOutputListeners().isEmpty() || appConfig.getErrorListeners().isEmpty()){
            throw new IllegalArgumentException("OutputStream Listener and ErrorStream Listener must be defined for SparkAppProxyStreamReaders");
        }
        SparkAppProxyImpl sparkAppProxy = (SparkAppProxyImpl) createSparkAppProxy(appConfig, true);
        StreamReader outReader = new StreamReader(sparkAppProxy.getInputStream());
        for(StreamListener outputListener : appConfig.getOutputListeners()){
            outReader.addObserver(outputListener);
        }
        StreamReader errorReader = new StreamReader(sparkAppProxy.getErrorStream());
        for(StreamListener errorListener : appConfig.getErrorListeners()){
            errorReader.addObserver(errorListener);
        }
        return new SparkAppProxyStreamReadersImpl(sparkAppProxy, outReader, errorReader);
    }

    private StreamListener buildErrorStreamListener(StringBuilder errorLog) {
        return new StreamListener
                    .Builder(s -> {
                        errorLog.append(s);
                        errorLog.append("\n");
                    })
                    .withErrorsOnlyFilter()
                    .withMaxLoggedLines(30)
                    .build();
    }

    private StreamListener buildOutputStreamListener(List<String> output) {
        return new StreamListener
                    .Builder(s -> {output.add(s);})
                    .build();
    }

    private SparkLauncher createSparkLauncher(String appName, String appResource, String mainClass, Map<String, String> conf, String[] appArgs, String sparkHome, String sparkMaster) {
        //validateInputs(appName, appResource, sparkHome, sparkMaster); UNCOMMENT this method in case you need to expose createSparkLauncher method to public interface.
        SparkLauncher sparkLauncher = new SparkLauncher();
        sparkLauncher.setVerbose(true)
                .setAppName(appName)
                .setMaster(sparkMaster)
                .setSparkHome(sparkHome)
                .setAppResource(appResource);
        if(isValidInput(mainClass)){
            sparkLauncher.setMainClass(mainClass);
        }
        if(!conf.isEmpty()){
            for (Map.Entry<String, String> entry: conf.entrySet()) {
                sparkLauncher.setConf(entry.getKey(), entry.getValue());
            }
        }
        if(appArgs != null){
            sparkLauncher.addAppArgs(appArgs);
        }
        return sparkLauncher;
    }

    private void validateInputs(String appName, String appResource, String sparkHome, String sparkMaster) {
        checkNotNullNorEmpty(appName, "appName");
        checkNotNullNorEmpty(appResource, "appResource");
        checkNotNullNorEmpty(sparkHome, "sparkHome");
        checkNotNullNorEmpty(sparkMaster, "sparkMaster");
    }

}
