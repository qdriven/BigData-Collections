package io.qkits.datasyncer.canal;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.CanalEntry.Column;
import com.alibaba.otter.canal.protocol.CanalEntry.Entry;
import com.alibaba.otter.canal.protocol.CanalEntry.EntryType;
import com.alibaba.otter.canal.protocol.CanalEntry.EventType;
import com.alibaba.otter.canal.protocol.CanalEntry.RowChange;
import com.alibaba.otter.canal.protocol.CanalEntry.RowData;
import com.alibaba.otter.canal.protocol.Message;

import io.qkits.datasyncer.utils.SyncerProperties;

/**
 * canal客户端处理线程
 *
 */
public class DataWorker extends Thread {

  private Logger logger = LoggerFactory.getLogger(DataWorker.class);
  private CanalServer canalServer = new CanalServer();
  private CountDownLatch countDownLatch = null;
  /**
   * mysql实例名称（与服务端对应）
   */
  private String destination = null;

  public DataWorker(CountDownLatch countDownLatch, String destination) {
    this.countDownLatch = countDownLatch;
    this.destination = destination;
  }

  @Override
  public void run() {
    logger.info(this.getName() + " 线程开始启动。countDownLatch = " + countDownLatch.getCount());
    try {
      CanalConnector connector = null;
      try {
        connector =
            CanalConnectors.newClusterConnector(SyncerProperties.zkServer, destination, "", "");
        connector.connect();
      } catch (Exception e) {
        logger.error(this.getName() + " 获取连接失败：", e);
        return;
      }
      long emptyCount = 0;
      while (true) {
        if (SyncerProperties.isStop) {
          logger.info(this.getName() + " 停止从canal服务中获取数据，isStop = " + SyncerProperties.isStop);
          return;
        }
        boolean b = true;
        long batchId = 0;
        try {
          // 获取指定数量的数据
          Message message = connector.getWithoutAck(SyncerProperties.CANAL_BATCH_SIZE);
          batchId = message.getId();
          List<Entry> entries = message.getEntries();
          if (batchId == -1 || entries.size() == 0) {
            emptyCount++;
            logger.info(this.getName() + " 从canal中获取数据为空的次数：" + emptyCount);
            try {
              Thread.sleep(SyncerProperties.CANAL_SLEEP_TIME);
            } catch (InterruptedException e) {
              logger.error(this.getName() + " 休眠时出现异常：", e);
            }
          } else {
            emptyCount = 0;
            b = processData(entries);
          }
          if (b) {
            // 提交确认
            connector.ack(batchId);
          } else {
            // 处理失败, 回滚数据
            System.out.println(this.getName() + " 数据处理失败，进行回滚，batchId = " + batchId);
            logger.info(this.getName() + " 数据处理失败，进行回滚，batchId = " + batchId);
            connector.rollback(batchId);
          }
        } catch (Exception e) {
          logger.error(this.getName() + " canal处理程序处理数据过程中出现异常：", e);
        }
      }
    } finally {
      countDownLatch.countDown();
      logger.info(this.getName() + " 线程结束。countDownLatch = " + countDownLatch.getCount());
    }
  }

  private boolean processData(List<Entry> entries) {
    boolean b = true;
    for (Entry entry : entries) {
      if (entry.getEntryType() == EntryType.TRANSACTIONBEGIN
          || entry.getEntryType() == EntryType.TRANSACTIONEND) {
        continue;
      }
      RowChange rowChage = null;
      try {
        rowChage = RowChange.parseFrom(entry.getStoreValue());
      } catch (Exception e) {
        logger.error(this.getName() + " 将entry格式化成RowChange时出现异常：", e);
        return false;
      }
      EventType eventType = rowChage.getEventType();
      String dbName = entry.getHeader().getSchemaName();
      String tableName = entry.getHeader().getTableName();
      List<Object> list = new ArrayList<Object>();
      for (RowData rowData : rowChage.getRowDatasList()) {
        String operType = "";
        if (eventType == EventType.DELETE) {
          operType = "DELETE";
        } else if (eventType == EventType.INSERT) {
          operType = "INSERT";
        } else if (eventType == EventType.UPDATE) {
          operType = "UPDATE";
        } else {
          continue;
        }
        String
            str =
            createJsonStr(dbName, tableName, operType, rowData.getBeforeColumnsList(),
                          rowData.getAfterColumnsList());
        list.add(str);
      }
      if (list.size() > 0) {
        b = canalServer.sendMessage(dbName, tableName, list);
      }
    }
    return b;
  }

  private String createJsonStr(String dbName, String tableName, String operType,
                               List<Column> beforeColumnsList, List<Column> afterColumnsList) {
    JSONObject afterColumnStr = getAfterColumnStr(dbName + "." + tableName, afterColumnsList);
    JSONObject beforeColumnStr = getBeforeColumnStr(dbName + "." + tableName, beforeColumnsList);
    List<String> updateColumnList = new ArrayList<String>();
    JSONObject obj = new JSONObject();
    if ("INSERT".equals(operType) || "UPDATE".equals(operType)) {
      Object id = afterColumnStr.get("id");
      if (null == id) {
        return null;
      }
      JSONObject afterJson = afterColumnStr.getJSONObject("data");
      Iterator<String> iter = afterJson.keySet().iterator();
      while (iter.hasNext()) {
        String key = iter.next();
        updateColumnList.add(key);
      }
      JSONObject beforeJson = new JSONObject();
      JSONObject jsonObject = beforeColumnStr.getJSONObject("data");
      iter = jsonObject.keySet().iterator();
      while (iter.hasNext()) {
        String key = iter.next();
        if (!updateColumnList.contains(key)) {
          continue;
        }
        beforeJson.put(key, (Object) jsonObject.get(key));
      }
      obj.put("id", id);
      obj.put("dbName", dbName);
      obj.put("tableName", tableName);
      obj.put("operType", operType);
      JSONObject dataObj = new JSONObject();
      dataObj.put(SyncerProperties.BEFORE_KEY, beforeJson);
      dataObj.put(SyncerProperties.AFTER_KEY, afterJson);
      obj.put("data", dataObj);
    } else {
      Object id = beforeColumnStr.get("id");
      JSONObject jsonObject = beforeColumnStr.getJSONObject("data");
      obj.put("id", id);
      obj.put("dbName", dbName);
      obj.put("tableName", tableName);
      obj.put("operType", operType);
      JSONObject dataObj = new JSONObject();
      dataObj.put(SyncerProperties.BEFORE_KEY, jsonObject);
      dataObj.put(SyncerProperties.AFTER_KEY, afterColumnStr.getJSONObject("data"));
      obj.put("data", dataObj);
    }
    return obj.toJSONString();
  }

  private JSONObject getBeforeColumnStr(String db, List<Column> beforeColumnsList) {
    Object id = 0;
    JSONObject beforeJson = new JSONObject();
    JSONObject beforeColumnObj = new JSONObject();
    for (Column column : beforeColumnsList) {
      if (column.getIsKey()) {
        try {
          id = Long.valueOf(column.getValue());
        } catch (Exception e) {
          id = column.getValue();
          logger.info(this.getName() + " " + db + "  主键为非数字类型，已使用原生主键类型,id = " + id);
        }
      }
      beforeColumnObj.put(column.getName().toLowerCase(), column.getValue());
    }
    beforeJson.put("id", id);
    beforeJson.put("data", beforeColumnObj);
    return beforeJson;
  }

  private JSONObject getAfterColumnStr(String db, List<Column> afterColumnsList) {
    Object id = 0;
    JSONObject afterJson = new JSONObject();
    JSONObject afterColumnObj = new JSONObject();
    for (Column column : afterColumnsList) {
      if (column.getIsKey()) {
        try {
          id = Long.valueOf(column.getValue());
        } catch (Exception e) {
          id = column.getValue();
          logger.info(this.getName() + " " + db + " 主键为非数字类型，已使用原生主键类型, id = " + id);
        }
      }
      afterColumnObj.put(column.getName().toLowerCase(), column.getValue());
    }
    afterJson.put("id", id);
    afterJson.put("data", afterColumnObj);
    return afterJson;
  }
}
