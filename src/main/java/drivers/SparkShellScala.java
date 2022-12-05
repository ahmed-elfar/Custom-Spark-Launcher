package drivers;

import com.xy.spark.launcher.*;
import com.xy.spark.launcher.proxy.SparkAppProxy;

import java.io.IOException;

import static drivers.TestAppInputs.sparkHome;
import static drivers.TestAppInputs.sparkMaster;

public class SparkShellScala {


    public static void main(String[] args) throws IOException, InterruptedException {

        final SparkAppState.Listener stateListener = new SparkAppState.Listener() {
            @Override
            public void onStateChanged(SparkAppState sparkAppState) {
                System.out.println("State from Spark Shell: " + sparkAppState);
            }
        };

        StreamListener outListener = new StreamListener
                .Builder(s -> {System.out.println("Out> " + s);})
                .build();
        StreamListener errorListener = new StreamListener
                .Builder(s -> {System.out.println("Error> " + s);})
                .build();

        SparkAppManager manager = SparkAppManagerFactory.getInstance();

        AppConfig appConfig = new AppConfig
                .Builder("Spark shell", "/spark-2.4.3/jars/spark-repl_2.11-2.4.3.jar")
                .withMainClass("org.apache.spark.repl.Main")
                .withSparkConfigs(TestAppInputs.getConf())
                .withSparkMaster(sparkMaster)
                .withSparkHome(sparkHome)
                .withSparkAppStateListener(stateListener)
                .withOutputStreamListener(outListener)
                .withErrorStreamListener(errorListener)
                .build();

        SparkAppProxy proxy = manager.submitAppWithStreamReaders("sparkShell", appConfig);

        Thread.sleep(10000);
        proxy.stop();
        manager.purge("sparkShell");
        System.out.println("DONE");
    }


}
