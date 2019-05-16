package io.qkits.sparklivy.config;

import lombok.Data;

@Data
public class SparkJobConfig {

    private String file;
    private String className;
    private String runSql;
    private String dfTableName;
    private String livyUri;
    private String proxyUser;
}
