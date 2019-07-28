package io.qkits.datasyncer.utils;


public class SyncerProperties {

  /**
   * 从canal服务中获取数据的状态标示（true：停止获取，结束线程；false：获取）
   */
  public static boolean isStop = false;
  /**
   * 一次从canal服务中获取多少条数据（有主函数设置，这里的值为默认值）
   */
  public static int CANAL_BATCH_SIZE = 100;
  /**
   * 从canal服务中获取数据为空时，休眠时间（有主函数设置，这里的值为默认值）
   */
  public static int CANAL_SLEEP_TIME = 1000;
  public static final String BEFORE_KEY = "before";
  public static final String AFTER_KEY = "after";

  /**
   * zookeeper服务地址
   */
  public static String zkServer = null;
  /**
   * kafka服务地址
   */
  public static String kafkaServer = null;

  public static int partition = 10;
}
