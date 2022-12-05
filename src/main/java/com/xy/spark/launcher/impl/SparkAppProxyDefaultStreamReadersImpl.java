package com.xy.spark.launcher.impl;

import com.xy.spark.launcher.SparkAppState;
import com.xy.spark.launcher.proxy.SparkAppProxyDefaultStreamReaders;
import com.xy.spark.launcher.proxy.SparkAppProxyStreamReaders;

import java.io.OutputStream;
import java.util.List;

class SparkAppProxyDefaultStreamReadersImpl implements SparkAppProxyDefaultStreamReaders {

    private final SparkAppProxyStreamReaders sparkAppProxy;
    private final List<String> output;
    private final StringBuilder errorLog;

    SparkAppProxyDefaultStreamReadersImpl(SparkAppProxyStreamReaders sparkAppProxy, List<String> output, StringBuilder errorLog){
        this.sparkAppProxy = sparkAppProxy;
        this.output = output;
        this.errorLog = errorLog;
    }

    @Override
    public OutputStream getProcessInput() {
        return this.sparkAppProxy.getProcessInput();
    }

    @Override
    public SparkAppState getState() {
        return this.sparkAppProxy.getState();
    }

    @Override
    public void stop() {
        sparkAppProxy.stop();
    }

    @Override
    public void stop(long timeout) {
        sparkAppProxy.stop(timeout);
    }

    @Override
    public void kill() {
        sparkAppProxy.kill();
    }

    @Override
    public void stopStreamReaders() {
        sparkAppProxy.stopStreamReaders();
    }

    @Override
    public boolean isTerminated() {
        return sparkAppProxy.isTerminated();
    }

    @Override
    public void awaitAppTermination() {
        sparkAppProxy.awaitAppTermination();
    }

    @Override
    public void waitTillRunning() {
        sparkAppProxy.waitTillRunning();
    }

    @Override
    public void waitTillRunning(long timeout) {
        sparkAppProxy.waitTillRunning(timeout);
    }

    @Override
    public List<String> getOutput() {
        return output;
    }

    @Override
    public String getDetailedErrorLog() {
        return errorLog.toString();
    }
}
