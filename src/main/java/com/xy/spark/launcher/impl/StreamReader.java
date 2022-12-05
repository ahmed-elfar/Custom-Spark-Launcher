package com.xy.spark.launcher.impl;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Observable;

class StreamReader extends Observable implements Runnable {

    private final InputStream is;

    public StreamReader(InputStream is) {
        this.is = is;
    }

    @Override
    public void run() {
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(is))) {
            String line = reader.readLine();
            while (line != null && !stopOnError(line)) {
                line = reader.readLine();
                setChanged();
                notifyObservers(line);
            }
        } catch (Exception e) {
            e.printStackTrace();                //TODO LOG
        }
    }

    private static boolean stopOnError(String line) {
        return line.contains("java.lang.OutOfMemoryError");
    }
}