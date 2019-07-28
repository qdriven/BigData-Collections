package io.qkits.datasyncer.canal;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.qkits.datasyncer.handlers.KafkaHandler;
import io.qkits.datasyncer.utils.SyncerProperties;


public class CanalServer {

  private Logger logger = LoggerFactory.getLogger(CanalServer.class);
  private KafkaHandler kafkaHandler = new KafkaHandler();

  public boolean sendMessage(String dbName, String tableName, List<Object> dataList) {
    int partition = Math.abs(tableName.hashCode() % SyncerProperties.partition);
    boolean b = false;
    for (int i = 0; i < 3; i++) {
      try {
        b = kafkaHandler.sendMessages(dbName, partition, dataList, true);
        logger.info("往kafka中发送消息，dbName.tableName = " + dbName + "." + tableName + "  partition = "
                    + partition + "  dataList.size = " + dataList.size());
      } catch (Exception e) {
        logger.error("消息发送到kafka时出现异常：", e);
      }
      if (b) {
        break;
      }
    }
    if (!b) {
      logger.error(
          "往kafka中发消息重试三次均失败，dbName = " + dbName + " tableName = " + tableName + " dataList = "
          + dataList.toString());
      return false;
    }
    return true;
  }
}
