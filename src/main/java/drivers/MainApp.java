package drivers;


import com.xy.spark.launcher.*;
import com.xy.spark.launcher.proxy.SparkAppProxy;
import com.xy.spark.launcher.proxy.SparkAppProxyStreamReaders;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.Map;

import static drivers.TestAppInputs.*;

public class MainApp {

    public static void main(String [] args) throws IOException, InterruptedException {

        int apps[] = new int[numberOfSparkApps];
        for(int i = 1; i <= numberOfSparkApps; i++){
            apps[i-1] = i;
        }
        System.out.println("================================================");
        System.out.println("Number of apps to test = " + numberOfSparkApps);
        System.out.println("================================================");

        final Map<String, String> conf = getConf();
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
                    StreamListener outListener = new StreamListener
                            .Builder(s -> {System.out.println("Out" + appID + "/ " + s);})
                            .build();
                    StreamListener errorListener = new StreamListener
                            .Builder(s -> {System.out.println("Error"+appID + "/ " + s);})
                            .withErrorsOnlyFilter()
                            //.withMaxLoggedLines(30)
                            .build();
                    AppConfig appConfig = new AppConfig
                            .Builder(appID + "", appResource)
                            .withMainClass(mainClass)
                            .withSparkConfigs(conf)
                            .withSparkMaster(sparkMaster)
                            .withSparkHome(sparkHome)
                            .withSparkAppStateListener(stateListener)
                            .withArgs(new String []{String.valueOf(appID)})
                            .withOutputStreamListener(outListener)
                            .withErrorStreamListener(errorListener)
                            .build();
                    SparkAppProxy proxy = manager.submitAppWithStreamReaders(appID + "", appConfig);
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
            SparkAppProxyStreamReaders proxy = (SparkAppProxyStreamReaders) manager.getSparkAppProxy(appKey).orElseGet(null);
            proxy.waitTillRunning();
            proxy.awaitAppTermination();
            proxy.stop();
            proxy.stopStreamReaders();
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
