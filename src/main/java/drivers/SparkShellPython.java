package drivers;

import com.xy.spark.launcher.*;
import com.xy.spark.launcher.proxy.SparkAppProxy;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;

import static drivers.TestAppInputs.sparkHome;
import static drivers.TestAppInputs.sparkMaster;

public class SparkShellPython {

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
                .Builder("PySparkShell", "pyspark-shell-main")
                .withSparkConfigs(TestAppInputs.getConf())
                .withSparkMaster(sparkMaster)
                .withSparkHome(sparkHome)
                .withSparkAppStateListener(stateListener)
                .withOutputStreamListener(outListener)
                .withErrorStreamListener(errorListener)
                .build();

        SparkAppProxy proxy = manager.submitAppWithStreamReaders("PySparkShell", appConfig);
        runInputToSparkAppThread(proxy);
        Thread.sleep(10000);
        proxy.stop();
        manager.purge("PySparkShell");
        System.out.println("\nDONE");

    }

    private static void runInputToSparkAppThread(SparkAppProxy proxy){
        Thread writerThread = new Thread(new Runnable() {
            @Override
            public void run() {
                try(BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(proxy.getProcessInput()))){
                    int x = 0;
                    String message = "Hello from MainApp";
                    while (!proxy.isTerminated()){
                        x++;
                        String messageNum =  "print('"+ message+ " : " + x + " To PySparkShell" + "')\n";
                        try {
                            bw.write(messageNum);
                            bw.flush();
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                        try {
                            Thread.sleep(500);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }
                    System.err.println("Terminated writer thread for : PySparkShell ");
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        });
        writerThread.start();
    }

}
