package com.xy.spark.launcher.impl;

import com.xy.spark.launcher.SparkAppState;
import org.apache.spark.launcher.SparkAppHandle;

import java.util.concurrent.atomic.AtomicBoolean;

class DefaultSparkListener implements SparkAppHandle.Listener {

    private volatile boolean isTerminated;
    private volatile SparkAppState state = SparkAppState.UNKNOWN;
    private final SparkAppState.Listener listener;

    public DefaultSparkListener(SparkAppState.Listener listener) {
        this.listener = listener;
    }

    public boolean isTerminated() {
        return isTerminated;
    }

    public SparkAppState getState() {
        return state;
    }

    @Override
    public void stateChanged(SparkAppHandle sparkAppHandle) {
        if(isTerminated){
            return;
        }
        if(sparkAppHandle.getState() == SparkAppHandle.State.SUBMITTED) {
            state = SparkAppState.WAITING;
            listener.onStateChanged(state);
        }else if(sparkAppHandle.getState() == SparkAppHandle.State.RUNNING){
            state = SparkAppState.RUNNING;
            listener.onStateChanged(state);
        }else if((sparkAppHandle.getState() == SparkAppHandle.State.FAILED
                || sparkAppHandle.getState() == SparkAppHandle.State.KILLED
                || sparkAppHandle.getState() == SparkAppHandle.State.LOST
                || sparkAppHandle.getState() == SparkAppHandle.State.FINISHED)){
            isTerminated = true;
            state = SparkAppState.TERMINATED;
            listener.onStateChanged(state);
        }
    }

    @Override
    public void infoChanged(SparkAppHandle sparkAppHandle) {

    }
}