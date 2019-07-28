package io.qkits.datasyncer.handlers;

import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.PartitionInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.qkits.datasyncer.utils.SyncerProperties;


public class KafkaHandler {

  private Logger logger = LoggerFactory.getLogger(KafkaHandler.class);
  private static final int RETRIES = 3;
  private static final int BATCH_SIZE = 16384;
  private static final int LINGER_MS = 1;
  private static final int BUFFER_MEMORY = 33554432;
  private Producer<String, String> producer = null;
  ;

  private void getProducerInstance() {
    try {
      Properties props = new Properties();
      props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, SyncerProperties.kafkaServer);
      // ACK ALL
      props.put(ProducerConfig.ACKS_CONFIG, "all");
      // retry times
      props.put(ProducerConfig.RETRIES_CONFIG, RETRIES);
      // Batch Size
      props.put(ProducerConfig.BATCH_SIZE_CONFIG, BATCH_SIZE);
      props.put(ProducerConfig.LINGER_MS_CONFIG, LINGER_MS);
      // Send in // memorys
      props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, BUFFER_MEMORY);
      props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringSerializer");
      props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringSerializer");
      producer = new KafkaProducer<String, String>(props);
    } catch (Exception e) {
      throw new RuntimeException(
          "init kafka producer fault, pls check server is ok or not...bootstrap_servers = "
          + SyncerProperties.kafkaServer);
    }
  }

  public boolean sendMessages(String topic, int partition, List<?> data, boolean isSync) {
    try {
      if (data.isEmpty()) {
        logger.info("sendMessage fault cause by no data,are you kidding me?");
        return true;
      }
      if (null == producer) {
        getProducerInstance();
      }
      List<PartitionInfo> partitionInfos = producer.partitionsFor(topic);
      for (Object obj : data) {
        String message = obj.toString();
        if (partition >= 0) {
          boolean faultPartition = true;
          for (PartitionInfo part : partitionInfos) {
            if (part.partition() == partition) {
              faultPartition = false;
              break;
            }
          }
          if (faultPartition) {
            logger.info("topic中没有该partition，partition = " + faultPartition);
            return false;
          }
          Future<RecordMetadata>
              send =
              producer.send(new ProducerRecord<String, String>(topic, partition, "key", message));
          if (isSync) {
            RecordMetadata recordMetadata = send.get(5, TimeUnit.SECONDS);
            recordMetadata.offset();
          }
        } else {
          int totalPartitionNum = partitionInfos.size();
          int myPartitionNum = new Random()
              .nextInt(totalPartitionNum);
          Future<RecordMetadata> send = producer
              .send(new ProducerRecord<String, String>(topic, myPartitionNum, "key", message));
          if (isSync) {
            RecordMetadata recordMetadata = send.get(5, TimeUnit.SECONDS);
            recordMetadata.offset();
          }
        }
      }
    } catch (Exception e) {
      if (null != producer) {
        producer.close();
        producer = null;
      }
      throw new RuntimeException(e);
    }
    return true;
  }
}
