package com.xy.spark.launcher;

import com.xy.spark.launcher.proxy.SparkAppProxy;
import com.xy.spark.launcher.proxy.SparkAppProxyDefaultStreamReaders;
import com.xy.spark.launcher.proxy.SparkAppProxyStreamReaders;

import java.io.IOException;
import java.util.Optional;

public interface SparkAppManager {

    Optional<SparkAppProxy> getSparkAppProxy(String appKey);

    SparkAppProxy submitApp(String appKey, AppConfig app) throws IOException;

    SparkAppProxyStreamReaders submitAppWithStreamReaders(String appKey, AppConfig app) throws IOException;

    SparkAppProxyDefaultStreamReaders submitAppWithDefaultStreamReaders(String appKey, AppConfig app) throws IOException;

    void purge(String appKey);

    SparkAppState getAppState(String appKey);

}
