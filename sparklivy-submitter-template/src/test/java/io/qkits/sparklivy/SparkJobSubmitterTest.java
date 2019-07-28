package io.qkits.sparklivy;

import io.qameta.allure.Epic;
import io.qameta.allure.Feature;
import io.qkits.sparklivy.config.SparkJobConfig;
import io.qkits.sparklivy.config.SparkJobConfigs;
import lombok.extern.slf4j.Slf4j;

import org.junit.Test;

import java.util.Map;


@Slf4j
@Epic("Submit Different Type Of Spark Job")
public class SparkJobSubmitterTest {

  @Test
  @Feature("同步获取Spark Job完成状态")
  public void testSparkJobSubmitter() {
    SparkJobSubmitter submitter = new SparkJobSubmitter();
    SparkJobConfig config = SparkJobConfigs.defaultSparkJobConfig();
    submitter.setLivyTaskSubmitHelper(new LivyTaskSubmitHelper());
    Map<String, Object> result = submitter.submitAndGetJobResult(config);
    System.out.println(result.get("id").toString());
  }

  @Test
  @Feature("异步提交Spark Job 任务,另外一个线程获取执行获取Job State信息,做进一步处理")
  public void testSparkSubmitSparkJob() {
    SparkJobSubmitter submitter = new SparkJobSubmitter();
    SparkJobConfig config = SparkJobConfigs.defaultSparkJobConfig();
    submitter.setLivyTaskSubmitHelper(new LivyTaskSubmitHelper());
    submitter.submitJob(config);
  }

}