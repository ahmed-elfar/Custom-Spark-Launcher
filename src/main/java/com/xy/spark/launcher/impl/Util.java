package com.xy.spark.launcher.impl;

import java.util.function.Supplier;

public class Util {

    public static final long BACKOFF_DELAY = 100;
    public static final long BACKOFF_TIMEOUT = 6000;

    public static void actionBackoff(Supplier<Boolean> stopCondition, Runnable action, long timeout) {
        long backOffAttempts = Math.max(1, timeout / BACKOFF_DELAY);
        actionWrapper(action, false);
        long attempts = 0;
        try {
            do {
                attempts++;
                Thread.sleep(BACKOFF_DELAY);
            } while (stopCondition.get() && attempts < backOffAttempts);
        } catch (Throwable e) {
        }
    }

    public static void actionWrapper(Runnable action, boolean shouldLog){
        try {
            action.run();
        } catch (Throwable e) {
            if(shouldLog){
                //TODO LOG
                //e.printStackTrace();
            }
        }
    }

    public static boolean isValidInput(String configValue){
        if(configValue != null && !configValue.isEmpty()) return true;
        return false;
    }

    public static void checkNotNullNorEmpty(String configValue, String configName) throws IllegalArgumentException {
        if (!isValidInput(configValue)) {
            throw new IllegalArgumentException(String.format("'%s' must not be null nor empty.", configName));
        }
    }

}
