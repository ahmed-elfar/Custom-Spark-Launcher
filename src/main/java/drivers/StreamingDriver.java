package drivers;


import com.xy.spark.launcher.AppConfig;
import com.xy.spark.launcher.SparkAppManager;
import com.xy.spark.launcher.SparkAppManagerFactory;
import com.xy.spark.launcher.SparkAppState;
import com.xy.spark.launcher.proxy.SparkAppProxy;
import com.xy.spark.launcher.proxy.SparkAppProxyDefaultStreamReaders;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;

import static drivers.TestAppInputs.*;

public class StreamingDriver {

    public static void main(String [] args) throws IOException, InterruptedException {

        int apps[] = new int[numberOfSparkApps];
        for(int i = 1; i <= numberOfSparkApps; i++){
            apps[i-1] = i;
        }
        System.out.println("================================================");
        System.out.println("Number of apps to test = " + numberOfSparkApps);
        System.out.println("================================================");

        SparkAppManager manager = SparkAppManagerFactory.getInstance();
        for (int appID : apps) {
            new Thread(() ->{
                final SparkAppState.Listener stateListener = new SparkAppState.Listener() {
                    @Override
                    public void onStateChanged(SparkAppState sparkAppState) {
                        System.out.println("State from AppID: "  + appID + " / " + sparkAppState);
                    }
                };
                try {
                    AppConfig appConfig = new AppConfig
                            .Builder(appID + "", appResource)
                            .withMainClass(mainClass)
                            .withSparkConfigs(getConf())
                            .withSparkHome(sparkHome)
                            .withSparkMaster(sparkMaster)
                            .withSparkAppStateListener(stateListener)
                            .withArgs(new String []{String.valueOf(appID)})
                            .build();
                    SparkAppProxyDefaultStreamReaders proxy = manager.submitAppWithDefaultStreamReaders(appID + "", appConfig);
                    if(testInputToSparkApp){
                        runInputToSparkAppThread(appID, proxy);
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }).start();
        }

        Thread.sleep(1000);

        for (int i : apps) {
            String appKey = i+"";
            SparkAppProxyDefaultStreamReaders proxy = (SparkAppProxyDefaultStreamReaders) manager.getSparkAppProxy(appKey).orElseGet(null);
            proxy.awaitAppTermination();
            proxy.stop();
            proxy.stopStreamReaders();
            System.out.println("\n============================ LOG For App: "+ i +" :===========================================");
            proxy.getOutput().forEach(x -> System.out.println(x));
            System.out.println("\n============================ Detailed ERROR LOG App: "+ i +" :================================");
            System.out.println(proxy.getDetailedErrorLog());
            proxy.stopStreamReaders();
        }

        for (int i : apps) {
            manager.purge(i+"");
            manager.purge(i+"");
        }
    }

    private static void runInputToSparkAppThread(int appID, SparkAppProxy proxy){
        Thread writerThread = new Thread(new Runnable() {
            @Override
            public void run() {
                try(BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(proxy.getProcessInput()))){
                    int x = 0;
                    String message = "Hello from MainApp";
                    while (!proxy.isTerminated()){
                        x++;
                        String messageNum = message+ " : " + x + " to AppID " + appID + "\n";
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
                    System.err.println("Terminated writer thread for : AppID " + appID);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        });
        writerThread.start();
    }

}
