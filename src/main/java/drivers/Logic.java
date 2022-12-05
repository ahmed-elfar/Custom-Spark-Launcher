package drivers;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;

import java.io.BufferedReader;
import java.io.InputStreamReader;

public class Logic {

    public static void main(String[] args) throws InterruptedException {

        Thread readerThread = new Thread(new Runnable() {
            @Override
            public void run() {

                try (BufferedReader reader = new BufferedReader(new InputStreamReader(System.in))) {
                    String line = reader.readLine();
                    while (line != null) {
                        System.out.println(line);
                        line = reader.readLine();
                    }

                } catch (Exception e) {
                    e.printStackTrace();
                }

            }
        });
        readerThread.setDaemon(true);
        readerThread.start();
        Thread.sleep(500);

        System.out.println("\n>>>>>>>> STARTING MAIN FROM APP "+ args[0] +" <<<<<<<<\n");
        SparkSession spark = SparkSession.builder().getOrCreate();
        JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());
        //jsc.setLogLevel("WARN");

        Dataset df = spark.read().parquet(TestAppInputs.parquetPath);
        //System.out.println("\n\n\n>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>After READ can reach that line.");
        if(args[0].equals("3")){
            throw new OutOfMemoryError("just for testing");
        }else if(args[0].equals("5")){
            int o = 5/0;
        }
        df.printSchema();

        int n =0;
        while(n < 5){
            n++;
            System.out.println("OUT: " + n + " / AppID: " + args[0]);
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }


    }

}
