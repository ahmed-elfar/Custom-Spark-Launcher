package com.xy.spark.launcher;

import com.xy.spark.launcher.impl.Util;

import javax.annotation.Nonnull;
import java.util.*;

public class AppConfig {

    public static class Builder {

        private AppConfig appConfig;

        public Builder(String appName, String appResource) {
            this.appConfig = new AppConfig(new HashMap<>(), new LinkedList<>(), new LinkedList<>());
            this.appConfig.appName = appName;
            this.appConfig.appResource = appResource;
        }

        public Builder withMainClass(String mainClass){
            this.appConfig.mainClass = mainClass;
            return this;
        }

        public Builder withSparkMaster(String sparkMaster) {
            this.appConfig.sparkMaster = sparkMaster;
            return this;
        }

        public Builder withSparkHome(String sparkHome){
            this.appConfig.sparkHome = sparkHome;
            return this;
        }

        public Builder withSparkConfig(String configKey, String configValue) {
            Util.checkNotNullNorEmpty(configKey, "Spark Config Key");
            Util.checkNotNullNorEmpty(configValue, "Spark Config value");
            this.appConfig.conf.put(configKey, configValue);
            return this;
        }

        public Builder withSparkConfigs(@Nonnull Map<String, String> configValues) {
            this.appConfig.conf.putAll(configValues);
            return this;
        }

        public Builder withSparkAppStateListener(@Nonnull SparkAppState.Listener stateListener) {
            this.appConfig.stateListener = stateListener;
            return this;
        }

        public Builder withArgs(@Nonnull String[] args) {
            if(args.length > 0) this.appConfig.appArgs = args;
            return this;
        }

        public Builder withOutputStreamListener(@Nonnull StreamListener... outputListener) {
            for (StreamListener sl: outputListener) {
                this.appConfig.outputListener.add(sl);
            }
            return this;
        }

        public Builder withErrorStreamListener(@Nonnull StreamListener... errorListener) {
            for (StreamListener sl: errorListener) {
                this.appConfig.errorListener.add(sl);
            }
            return this;
        }

        public AppConfig build() {
            Util.checkNotNullNorEmpty(appConfig.appName, "appName");
            Util.checkNotNullNorEmpty(appConfig.appResource, "appResource");
            Util.checkNotNullNorEmpty(appConfig.sparkHome, "sparkHome");
            Util.checkNotNullNorEmpty(appConfig.sparkMaster, "sparkMaster");
            if(appConfig.stateListener == null){
                throw new IllegalArgumentException("SparkAppState.Listener cannot be null");
            }
            return appConfig;
        }

    }

    private String appName;
    private String appResource;
    private String mainClass;
    private String sparkHome;
    private String sparkMaster;
    private String[] appArgs;
    private final Map<String, String> conf;
    private SparkAppState.Listener stateListener;
    private final List<StreamListener> outputListener;
    private final List<StreamListener> errorListener;

    private AppConfig(Map<String, String> conf, List<StreamListener> outputListener, List<StreamListener> errorListener) {
        this.conf = conf;
        this.outputListener = outputListener;
        this.errorListener = errorListener;
    }

    public String getAppName() {
        return appName;
    }

    public String getAppResource() {
        return appResource;
    }

    public String getMainClass() {
        return mainClass;
    }

    public Map<String, String> getConf() {
        return Collections.unmodifiableMap(conf);
    }

    public String getSparkHome() {
        return sparkHome;
    }

    public String getSparkMaster() {
        return sparkMaster;
    }

    public SparkAppState.Listener getStateListener() {
        return stateListener;
    }

    public String[] getAppArgs() {
        return appArgs;
    }

    public List<StreamListener> getOutputListeners() {
        return outputListener;
    }

    public List<StreamListener> getErrorListeners() {
        return errorListener;
    }
}
