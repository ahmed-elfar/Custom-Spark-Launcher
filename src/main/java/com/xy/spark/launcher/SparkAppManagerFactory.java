package com.xy.spark.launcher;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

public class SparkAppManagerFactory {

    public static SparkAppManager getInstance(){
        try {
            Method method = Class.forName("com.xy.spark.launcher.impl.SparkAppManagerImpl")
                    .getDeclaredMethod("getInstance");
            method.setAccessible(true);
            return (SparkAppManager) method.invoke(null);
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (NoSuchMethodException e) {
            e.printStackTrace();
        } catch (InvocationTargetException e) {
            e.printStackTrace();
        }
        return null;
    }

}
