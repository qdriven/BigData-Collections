package io.qkits.sparklivy;

import lombok.Data;
import org.springframework.context.annotation.Configuration;

@Configuration
@Data
public class SparkDefaultConf {

    private String file;
    private String className;
    private String proxyUsers ;
}
