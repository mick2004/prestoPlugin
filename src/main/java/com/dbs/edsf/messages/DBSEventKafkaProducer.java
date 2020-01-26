package com.dbs.edsf.messages;

import java.util.Properties;
import org.apache.hadoop.conf.Configuration;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.internals.ErrorLoggingCallback;

public class DBSEventKafkaProducer {
   private Producer<String, String> producer;
   private String topic;

   public DBSEventKafkaProducer(Configuration configuration) {
      Properties properties = new Properties();
      properties.put("bootstrap.servers", configuration.get("DBS.kafka.metadata.broker.list"));
      properties.put("key.serializer", configuration.get("DBS.kafka.key.serializer.class"));
      properties.put("value.serializer", configuration.get("DBS.kafka.value.serializer.class"));
      properties.put("acks", configuration.get("DBS.kafka.request.required.acks"));
      properties.put("retries", configuration.get("DBS.kafka.message.send.max.retries"));
      properties.put("retry.backoff.ms", configuration.get("DBS.kafka.retry.backoff.ms"));
      properties.put("batch.size", configuration.get("DBS.kafka.batch.num.messages"));
      properties.put("reconnect.backoff.ms", configuration.get("DBS.kafka.reconnect.backoff.ms"));
      properties.put("max.block.ms", configuration.get("DBS.kafka.message.send.max.block.ms"));
      properties.put("request.timeout.ms", configuration.get("DBS.kafka.request.timeout.ms"));
      this.producer = new KafkaProducer(properties);
      this.topic = configuration.get("DBS.kafka.topic");
   }

   public void write(String messageJson) throws Exception {
      ProducerRecord<String, String> message = new ProducerRecord(this.topic, messageJson);
      byte[] keyBytes = message.key() == null ? null : ((String)message.key()).getBytes();
      byte[] valueBytes = message.value() == null ? null : ((String)message.value()).getBytes();
      this.producer.send(message, new ErrorLoggingCallback(this.topic, keyBytes, valueBytes, true));
   }

   public void close() {
      this.producer.close();
   }
}
