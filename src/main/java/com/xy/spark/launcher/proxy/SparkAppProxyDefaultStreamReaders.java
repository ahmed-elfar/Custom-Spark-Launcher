package com.xy.spark.launcher.proxy;

import java.util.List;

public interface SparkAppProxyDefaultStreamReaders extends SparkAppProxyStreamReaders {

    List<String> getOutput();

    String getDetailedErrorLog();
}
