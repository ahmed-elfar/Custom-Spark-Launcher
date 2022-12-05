package com.xy.spark.launcher.impl;

import com.xy.spark.launcher.SparkAppState;
import com.xy.spark.launcher.proxy.SparkAppProxy;
import com.xy.spark.launcher.proxy.SparkAppProxyStreamReaders;

import java.io.OutputStream;

import static com.xy.spark.launcher.impl.Util.actionWrapper;

class SparkAppProxyStreamReadersImpl implements SparkAppProxyStreamReaders {

    private final SparkAppProxy sparkAppProxy;
    private final Thread outputReaderThread;
    private final Thread errorReaderThread;

    SparkAppProxyStreamReadersImpl(SparkAppProxy sparkAppProxy, StreamReader outputReader, StreamReader errorReader) {
        this.sparkAppProxy = sparkAppProxy;
        outputReaderThread = new Thread(outputReader);
        errorReaderThread = new Thread(errorReader);
        outputReaderThread.start();
        errorReaderThread.start();
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
    public boolean isTerminated() {
        return sparkAppProxy.isTerminated();
    }

    @Override
    public void awaitAppTermination() {
        sparkAppProxy.awaitAppTermination();
        try {
            outputReaderThread.join();
        } catch (InterruptedException e) {
            //TODO LOG
            e.printStackTrace();
        }
        try {
            errorReaderThread.join();
        } catch (InterruptedException e) {
            //TODO LOG
            e.printStackTrace();
        }
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
    public void stopStreamReaders() {
        actionWrapper(() -> outputReaderThread.interrupt(), false);
        actionWrapper(() -> errorReaderThread.interrupt(), false);
    }
}
