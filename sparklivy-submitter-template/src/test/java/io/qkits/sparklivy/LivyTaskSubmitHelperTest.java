package io.qkits.sparklivy;

import io.qameta.allure.Epic;
import io.qameta.allure.Feature;
import org.assertj.core.api.Assertions;
import org.junit.Test;

import java.util.Map;

@Epic("livy Task Submitter Test Suites")
public class LivyTaskSubmitHelperTest {

    LivyTaskSubmitHelper livyTaskSubmitHelper = new LivyTaskSubmitHelper();

    @Test
    @Feature("通过Livy Server创建Spark任务成功")
    public void testCreateBatch(){

        SparkJobConfig config = SparkJobConfigs.defaultSparkJobConfig();
        Map<String, Object> batchResult = livyTaskSubmitHelper.createBatch(config);
        Integer id = Integer.parseInt(batchResult.get("id").toString());
        Assertions.assertThat(id).isGreaterThan(1);

    }

    @Feature("通过Livy Server 获取Spark 任务状态")
    @Test
    public void testGetJobState(){
        SparkJobConfig config = SparkJobConfigs.defaultSparkJobConfig();
        Map<String, Object> batchResult = livyTaskSubmitHelper.createBatch(config);
        Map<String, Object> result = livyTaskSubmitHelper.getBatchStatus(batchResult.get("id").toString(),config);
        System.out.println(result);
        Assertions.assertThat(result.get("state")).isEqualTo(LivySessionStates.State.RUNNING.name().toLowerCase());
    }
}