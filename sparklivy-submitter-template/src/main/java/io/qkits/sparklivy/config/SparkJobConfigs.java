package io.qkits.sparklivy.config;

public class SparkJobConfigs {


    public static SparkJobConfig defaultSparkJobConfig(){

        SparkJobConfig config = new SparkJobConfig();
        config.setFile("/Users/patrick/daily/qe-path/testercloud-dq/spark-dqsql-app/target/spark-dqsql-app-1.0-SNAPSHOT.jar");
        config.setClassName("io.testercloud.spark.DQSparkApp");
        config.setDfTableName("data_connector");
        config.setRunSql("select * from data_connector where isIdCardNo(ERRORNUM) !=0");
        config.setLivyUri("http://localhost:8998/batches");
        return config;
    }
}
