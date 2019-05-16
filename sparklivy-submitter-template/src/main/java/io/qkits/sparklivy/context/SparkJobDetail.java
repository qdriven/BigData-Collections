package io.qkits.sparklivy.context;

import com.bkjk.credit.testsupport.submitter.config.SparkJobConfig;
import lombok.Builder;
import lombok.Data;
import lombok.ToString;

@Data
@ToString
@Builder
public class SparkJobDetail {

    private Integer sparkJobId;
    private Integer sparkJobStatus;
    private SparkJobConfig config;
}
