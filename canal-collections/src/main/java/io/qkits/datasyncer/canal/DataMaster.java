package io.qkits.datasyncer.canal;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.TimeZone;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.PropertyConfigurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.qkits.datasyncer.utils.SyncerProperties;


public class DataMaster {

  private static Logger logger = LoggerFactory.getLogger(DataMaster.class);

  public static void initLog4J() throws FileNotFoundException {
    TimeZone tz = TimeZone.getTimeZone("GMT+8");
    TimeZone.setDefault(tz);
    String dir = System.getProperty("user.dir");
    PropertyConfigurator.configure(dir + "/config/log4j.properties");
  }

  public static void main(String[] args) throws FileNotFoundException {
    initLog4J();
    String destinationsStr = "";
    try {
      destinationsStr = getConfig();
      if (StringUtils.isEmpty(destinationsStr)) {
        logger.error("destinations为空，程序不能执行");
        System.out.println("destinations为空，程序不能执行");
        return;
      }
      if (StringUtils.isEmpty(SyncerProperties.zkServer) || StringUtils
          .isEmpty(SyncerProperties.kafkaServer)) {
        logger.error("zkServer或kafkaServer地址为空！");
        System.out.println("zkServer或kafkaServer地址为空！");
        return;
      }
    } catch (IOException e) {
      logger.error("获取配置文件出现异常：", e);
      System.out.println("获取配置文件出现异常：");
      e.printStackTrace();
    }
    startRun(destinationsStr);
  }

  private static void startRun(String destinationsStr) {
    logger.info("程序开始执行");
    String[] destinations = destinationsStr.split(",");
    int threadNum = destinations.length;
    ExecutorService executorServices = Executors.newFixedThreadPool(threadNum);
    CountDownLatch count = new CountDownLatch(threadNum);
    for (String destination : destinations) {
      DataWorker DataWorker = new DataWorker(count, destination);
      DataWorker.setName(destination);
      executorServices.submit(DataWorker);
    }
    try {
      count.await();
      executorServices.shutdown();
    } catch (Exception e) {
      logger.error("等待线程结束时出现异常：", e);
    }
    logger.info("程序结束执行");
  }

  private static String getConfig() throws IOException {
    String dir = System.getProperty("user.dir");
    Properties props = new Properties();
    InputStream in = new FileInputStream(new File(dir, "config/canal.properties"));
    props.load(in);
    String isStop = props.getProperty("isStop");
    if ("true".equals(isStop)) {
      SyncerProperties.isStop = true;
    } else {
      SyncerProperties.isStop = false;
    }
    String canalBatchSize = props.getProperty("canal_batch_size");
    if (!StringUtils.isEmpty(canalBatchSize)) {
      SyncerProperties.CANAL_BATCH_SIZE = Integer.parseInt(canalBatchSize);
    }
    String canalSleepTime = props.getProperty("canal_sleep_time");
    if (!StringUtils.isEmpty(canalSleepTime)) {
      SyncerProperties.CANAL_SLEEP_TIME = Integer.parseInt(canalSleepTime);
    }
    SyncerProperties.zkServer = props.getProperty("zkServer");
    SyncerProperties.kafkaServer = props.getProperty("kafkaServer");
    String partition = props.getProperty("partition");
    if (!StringUtils.isEmpty(partition)) {
      SyncerProperties.partition = Integer.parseInt(partition);
    }
    String destinations = props.getProperty("destinations");
    return destinations;
  }

}
