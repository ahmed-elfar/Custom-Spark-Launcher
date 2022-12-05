package drivers;

import org.apache.spark.launcher.SparkLauncher;

import java.io.IOException;

public class Fail {

    public static void main(String[] args) throws InterruptedException {
        String applicationName = "fail";
            String appName = "myApp:(" + applicationName +")";
            String sparkHome = TestAppInputs.sparkHome;
            String sparkMaster = TestAppInputs.sparkMaster;
            String appResource = TestAppInputs.appResource;

            String mainClass = "custom_launcher.driver.Logic";
            String []  appArgs ={applicationName};

        try {
            new SparkLauncher()
                    .setVerbose(true)
                    .setAppName(appName)
                    .setMaster(sparkMaster)
                    .setSparkHome(sparkHome)
                    .setAppResource(appResource) // "/my/app.jar"
                    .setMainClass(mainClass) // "my.spark.app.Main";
                    .setConf("spark.driver.memory", "1g")
                    .setConf("spark.cores.max","1")
                    .setConf("spark.executor.memory","1g")
                    .setConf("spark.executor.cores","1")
                    .setConf("spark.sql.shuffle.partitions","12")
                    //.setDeployMode("cluster")
                    .addAppArgs(appArgs).startApplication();
        } catch (IOException e) {
            e.printStackTrace();
        }
        Thread.sleep(5000);

        }

}
