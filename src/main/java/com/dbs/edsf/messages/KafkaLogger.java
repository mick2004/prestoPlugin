package com.dbs.edsf.messages;

import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class KafkaLogger {
   private static KafkaLogger INSTANCE;
   private static final Logger LOG = LoggerFactory.getLogger(KafkaLogger.class);
   private DBSEventKafkaProducer DBSProducer;

   private KafkaLogger(Configuration configuration) {
      ClassLoader currentCL = Thread.currentThread().getContextClassLoader();

      try {
         Thread.currentThread().setContextClassLoader((ClassLoader)null);
         this.DBSProducer = new DBSEventKafkaProducer(configuration);
      } catch (Exception var7) {
         LOG.error("DBSEventKafkaProducer could not be initialized", var7);
      } finally {
         Thread.currentThread().setContextClassLoader(currentCL);
      }

   }

   public static synchronized KafkaLogger getInstance(Configuration configuration) {
      if (INSTANCE == null) {
         INSTANCE = new KafkaLogger(configuration);
      }

      return INSTANCE;
   }

   public void log(String message) {
      try {
         if (this.DBSProducer != null) {
            this.DBSProducer.write(message);
         }
      } catch (Exception var3) {
         LOG.error(" Kafka seems to be down, or DBS plugin cannot connect to Kafka broker ", var3);
      }

   }
}
