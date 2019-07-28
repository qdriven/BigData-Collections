package io.qkits.datasyncer;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.TreeMap;

import org.apache.kafka.common.security.JaasUtils;

import kafka.admin.AdminUtils;
import kafka.admin.RackAwareMode;
import kafka.api.PartitionOffsetRequestInfo;
import kafka.common.ErrorMapping;
import kafka.common.OffsetMetadataAndError;
import kafka.common.TopicAndPartition;
import kafka.javaapi.OffsetFetchRequest;
import kafka.javaapi.OffsetFetchResponse;
import kafka.javaapi.OffsetResponse;
import kafka.javaapi.PartitionMetadata;
import kafka.javaapi.TopicMetadata;
import kafka.javaapi.TopicMetadataRequest;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.network.BlockingChannel;
import kafka.server.ConfigType;
import kafka.utils.ZkUtils;


public class App {

  public static void main(String[] args) {
    // getAllTopics("datainfra-test1:2182,datainfra-test2:2182,datainfra-test3:2182",
    // "forder");
    // createTopic("datainfra-test1:2182,datainfra-test2:2182,datainfra-test3:2182",
    // "test_2", 10, 2);
    // deleteTopic("datainfra-test1:2181,datainfra-test2:2181,datainfra-test3:2181",
    // "test_1");

    String topic = "lft";
    String broker = "10.241.0.41";
    int port = 9092;
    String group = "flink-10";
    String clientId = "";
    int correlationId = 0;
    BlockingChannel
        channel =
        new BlockingChannel(broker, port, BlockingChannel.UseDefaultBufferSize(),
                            BlockingChannel.UseDefaultBufferSize(), 5000);
    channel.connect();

    List<String> seeds = new ArrayList<String>();
    seeds.add(broker);
    App kot = new App();

    TreeMap<Integer, PartitionMetadata> metadatas = kot.findLeader(seeds, port, topic);

    long sum = 0l;
    long sumOffset = 0l;
    long lag = 0l;
    List<TopicAndPartition> partitions = new ArrayList<TopicAndPartition>();
    for (Entry<Integer, PartitionMetadata> entry : metadatas.entrySet()) {
      int partition = entry.getKey();
      TopicAndPartition testPartition = new TopicAndPartition(topic, partition);
      partitions.add(testPartition);
    }
    OffsetFetchRequest
        fetchRequest =
        new OffsetFetchRequest(group, partitions, (short) 0, correlationId, clientId);
    for (Entry<Integer, PartitionMetadata> entry : metadatas.entrySet()) {
      int partition = entry.getKey();
      try {
        channel.send(fetchRequest.underlying());
        OffsetFetchResponse
            fetchResponse =
            OffsetFetchResponse.readFrom(channel.receive().payload());
        TopicAndPartition testPartition0 = new TopicAndPartition(topic, partition);
        OffsetMetadataAndError result = fetchResponse.offsets().get(testPartition0);
        short offsetFetchErrorCode = result.error().code();
        if (offsetFetchErrorCode == ErrorMapping.NotCoordinatorForConsumerCode()) {
        } else {
          long retrievedOffset = result.offset();
          sumOffset += retrievedOffset;
        }
        String leadBroker = entry.getValue().leader().host();
        String clientName = "Client_" + topic + "_" + partition;
        SimpleConsumer
            consumer =
            new SimpleConsumer(leadBroker, port, 100000, 64 * 1024, clientName);
        long
            readOffset =
            getLastOffset(consumer, topic, partition, kafka.api.OffsetRequest.LatestTime(),
                          clientName);
        sum += readOffset;
        System.out.println(partition + ":" + readOffset);
        if (consumer != null) {
          consumer.close();
        }
      } catch (Exception e) {
        channel.disconnect();
      }
    }

    System.out.println("logSize：" + sum);
    System.out.println("offset：" + sumOffset);

    lag = sum - sumOffset;
    System.out.println("lag:" + lag);
  }

  public static long getLastOffset(SimpleConsumer consumer, String topic, int partition,
                                   long whichTime,
                                   String clientName) {
    TopicAndPartition topicAndPartition = new TopicAndPartition(topic, partition);
    Map<TopicAndPartition, PartitionOffsetRequestInfo>
        requestInfo =
        new HashMap<TopicAndPartition, PartitionOffsetRequestInfo>();
    requestInfo.put(topicAndPartition, new PartitionOffsetRequestInfo(whichTime, 1));
    kafka.javaapi.OffsetRequest request = new kafka.javaapi.OffsetRequest(requestInfo,
                                                                          kafka.api.OffsetRequest
                                                                              .CurrentVersion(),
                                                                          clientName);
    OffsetResponse response = consumer.getOffsetsBefore(request);
    if (response.hasError()) {
      System.out.println(
          "Error fetching data Offset Data the Broker. Reason: " + response
              .errorCode(topic, partition));
      return 0;
    }
    long[] offsets = response.offsets(topic, partition);
    return offsets[0];
  }

  private TreeMap<Integer, PartitionMetadata> findLeader(List<String> a_seedBrokers, int a_port,
                                                         String a_topic) {
    TreeMap<Integer, PartitionMetadata> map = new TreeMap<Integer, PartitionMetadata>();
    loop:
    for (String seed : a_seedBrokers) {
      SimpleConsumer consumer = null;
      try {
        consumer =
            new SimpleConsumer(seed, a_port, 100000, 64 * 1024,
                               "leaderLookup" + new Date().getTime());
        List<String> topics = Collections.singletonList(a_topic);
        TopicMetadataRequest req = new TopicMetadataRequest(topics);
        kafka.javaapi.TopicMetadataResponse resp = consumer.send(req);

        List<TopicMetadata> metaData = resp.topicsMetadata();
        for (TopicMetadata item : metaData) {
          for (PartitionMetadata part : item.partitionsMetadata()) {
            map.put(part.partitionId(), part);
          }
        }
      } catch (Exception e) {
        System.out
            .println("Error communicating with Broker [" + seed + "] to find Leader for [" + a_topic
                     + ", ] Reason: " + e);
      } finally {
        if (consumer != null) {
          consumer.close();
        }
      }
    }
    return map;
  }

  private static long getOffsetByGroup() {
    return 0;
  }

  private static void createTopic(String zkServer, String topic, int partition, int replication) {
    ZkUtils zkUtils = ZkUtils.apply(zkServer, 30000, 30000, JaasUtils.isZkSecurityEnabled());
    AdminUtils.createTopic(zkUtils, topic, partition, replication, new Properties(),
                           RackAwareMode.Enforced$.MODULE$);
    zkUtils.close();
  }

  private static void deleteTopic(String zkServer, String topic) {
    ZkUtils zkUtils = ZkUtils.apply(zkServer, 30000, 30000, JaasUtils.isZkSecurityEnabled());
    AdminUtils.deleteTopic(zkUtils, topic);
    zkUtils.close();
  }

  private static void getAllTopics(String zkServer, String topic) {
    ZkUtils zkUtils = ZkUtils.apply(zkServer, 30000, 30000, JaasUtils.isZkSecurityEnabled());
    // 获取topic 'test'的topic属性属性
    Properties props = AdminUtils.fetchEntityConfig(zkUtils, ConfigType.Topic(), topic);
    // 查询topic-level属性
    Iterator it = props.entrySet().iterator();
    while (it.hasNext()) {
      Map.Entry entry = (Map.Entry) it.next();
      Object key = entry.getKey();
      Object value = entry.getValue();
      System.out.println(key + " = " + value);
    }
    zkUtils.close();
  }
}
