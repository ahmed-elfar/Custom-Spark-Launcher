package drivers;

import java.util.HashMap;
import java.util.Map;

public class TestAppInputs {

    public static final String HOME_PATH = "";
    public static final int numberOfSparkApps = 1;
    public static final String sparkHome = HOME_PATH + "/spark-2.4.3";
    public static final String appResource = HOME_PATH + "1.0-SNAPSHOT.jar";
    public static final String parquetPath = "";
    public static final String sparkMaster = "spark://localhost:7077";

    public static final String mainClass = "drivers.Logic";
    public static final boolean testInputToSparkApp = true; //If true it will start a thread for each spark app which writes messages to its sparkApp

    public static Map<String, String> getConf() {
        Map conf = new HashMap();
        conf.put("spark.driver.memory", "1g");
        conf.put("spark.cores.max","1");
        conf.put("spark.executor.memory","1g");
        conf.put("spark.executor.cores","1");
        conf.put("spark.sql.shuffle.partitions","12");

//        conf.put("spark.mv.appleUser", "John");
//        conf.put("spark.mv.applePassword", "pass");
//        conf.put("spark.mv.appleDNS", "8.8.8.8");
//        conf.put("spark.mv.secret", "apple");
        //conf.put("spark.redaction.regex", "(?i)secret|appleuser|applepassword|appleDNS|dir|master|jars");
        //conf.put("spark.redaction.regex", "*");
        return conf;
    }

}
