package com.dbs.edsf.presto.metrics;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.METHOD})
public @interface Metric {
   String key() default "";

   Metric.TYPE type() default Metric.TYPE.COUNTER;

   public static enum TYPE {
      COUNTER,
      TIMER;
   }
}
