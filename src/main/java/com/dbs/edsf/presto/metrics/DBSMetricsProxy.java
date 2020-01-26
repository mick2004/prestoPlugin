package com.dbs.edsf.presto.metrics;

import com.timgroup.statsd.StatsDClient;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;

public class DBSMetricsProxy implements InvocationHandler {
   private Object obj;
   private StatsDClient statsdClient;

   private DBSMetricsProxy(Object obj) {
      this.obj = obj;
      this.statsdClient = DBSStatsdClient.getInstance();
   }

   private DBSMetricsProxy(Object obj, StatsDClient statsdClient) {
      this.obj = obj;
      this.statsdClient = statsdClient;
   }

   public Object invoke(Object proxy, Method m, Object[] args) throws Throwable {
      try {
         Metric[] metrics = (Metric[])m.getAnnotationsByType(Metric.class);
         long t1 = System.currentTimeMillis();
         Object result = m.invoke(this.obj, args);
         long t2 = System.currentTimeMillis();
         if (metrics != null && metrics.length > 0) {
            Metric[] var10 = metrics;
            int var11 = metrics.length;

            for(int var12 = 0; var12 < var11; ++var12) {
               Metric metric = var10[var12];
               Metric.TYPE type = metric.type();
               String key = metric.key();
               switch(type) {
               case COUNTER:
                  this.statsdClient.incrementCounter(key);
                  break;
               case TIMER:
                  this.statsdClient.recordExecutionTime(key, t2 - t1);
               }
            }
         }

         return result;
      } catch (InvocationTargetException var16) {
         throw var16.getCause();
      }
   }

   public static <T> T newInstance(T obj, Class<T> iface) {
      return (T) Proxy.newProxyInstance(iface.getClassLoader(), new Class[]{iface}, new DBSMetricsProxy(obj));
   }

   public static <T> T newInstance(T obj, Class<T> iface, StatsDClient statsdClient) {
      return (T) Proxy.newProxyInstance(iface.getClassLoader(), new Class[]{iface}, new DBSMetricsProxy(obj, statsdClient));
   }
}
