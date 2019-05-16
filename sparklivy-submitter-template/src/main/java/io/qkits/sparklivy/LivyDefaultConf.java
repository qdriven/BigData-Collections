package io.qkits.sparklivy;

import lombok.Data;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;

@Configuration
@Data
public class LivyDefaultConf {

    @Value("livy.uri:http://localhost:8998/batches")
    private String livyURI;

    @Value("livy.need.queue: false")
    private boolean livyNeedQueue;

    @Value("livy.task.max.concurrent.count:20")
    private String livyTaskMaxConcurrentCount ;

    @Value("livy.task.submit.interval.second: 3")
    private String livytaskSubmitIntervalSecond;

    @Value("livy.task.appId.retry.count: 3")
    private String livyTaskAppIdRetryCount ;

}
