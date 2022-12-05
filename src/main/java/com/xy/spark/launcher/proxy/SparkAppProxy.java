package com.xy.spark.launcher.proxy;

import com.xy.spark.launcher.SparkAppState;

import java.io.OutputStream;

public interface SparkAppProxy {

    OutputStream getProcessInput();

    SparkAppState getState();

    void stop();

    void stop(long timeout);

    void kill();

    boolean isTerminated();

    void awaitAppTermination();

    void waitTillRunning();

    void waitTillRunning(long timeout);

}
