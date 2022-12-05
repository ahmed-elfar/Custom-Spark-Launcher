package com.xy.spark.launcher.impl;

import com.xy.spark.launcher.SparkAppState;
import com.xy.spark.launcher.proxy.SparkAppProxy;
import org.apache.spark.launcher.SparkAppHandle;

import java.io.InputStream;
import java.io.OutputStream;

import static com.xy.spark.launcher.impl.Util.*;

class SparkAppProxyImpl implements SparkAppProxy {

    private final SparkAppHandle sparkAppHandle;
    private final DefaultSparkListener sparkAppListener;

    SparkAppProxyImpl(DefaultSparkListener sparkAppListener, SparkAppHandle sparkAppHandle) {
        this.sparkAppListener = sparkAppListener;
        this.sparkAppHandle = sparkAppHandle;
    }

    @Override
    public OutputStream getProcessInput() {
        return sparkAppHandle.getOutputStream();
    }

    public SparkAppState getState() {
        return sparkAppListener.getState();
    }

    public void stop() {
        stopInternal(0);
    }

    @Override
    public void stop(long timeout) {
        stopInternal(timeout);
    }

    private void stopInternal(long timeout) {
        long to = timeout > 0 ? timeout : BACKOFF_TIMEOUT;
        try{
            if(!isTerminated()) {
                actionBackoff(() -> !sparkAppListener.isTerminated(), () -> sparkAppHandle.stop(), to);
            }
        }finally {
            kill();
        }
    }

    public boolean isTerminated(){
        return sparkAppListener.isTerminated();
    }

    public void awaitAppTermination() {
        actionBackoff(() -> !isTerminated(), ()-> {}, Long.MAX_VALUE -1);
    }

    @Override
    public void waitTillRunning() {
        waitTillRunning(0);
    }

    @Override
    public void waitTillRunning(long timeout) {
        if(timeout <= 0){
            timeout = Long.MAX_VALUE -1;
        }
        actionBackoff(()-> sparkAppListener.getState() != SparkAppState.RUNNING && !sparkAppListener.isTerminated(), ()-> {},timeout);
    }

    public void kill(){
        actionWrapper(() -> sparkAppHandle.kill(), false);
    }

    InputStream getInputStream(){
        return sparkAppHandle.getInputStream();
    }

    InputStream getErrorStream(){
        return sparkAppHandle.getErrorStream();
    }
}