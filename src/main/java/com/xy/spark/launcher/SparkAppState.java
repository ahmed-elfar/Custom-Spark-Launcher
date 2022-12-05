package com.xy.spark.launcher;

public enum SparkAppState {

    NO_APPLICATION("No Application Found"),
    WAITING("Waiting for Resources! or trying to connect to Spark master!, Please hold on or try again later"),
    RUNNING("Running"),
    TERMINATED("Terminated"),
    UNKNOWN("Unknown State");

    private String stateMessage;

    SparkAppState(String stateMessage){
        this.stateMessage = stateMessage;
    }

    public String getStateMessage(){
        return stateMessage;
    }

    public interface Listener {

        void onStateChanged(SparkAppState sparkAppState);
    }
}
