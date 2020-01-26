package com.dbs.edsf.presto.metrics;

import com.dbs.edsf.presto.authorization.policy.DBSPrestoConfiguration;
import com.timgroup.statsd.NoOpStatsDClient;
import com.timgroup.statsd.NonBlockingStatsDClient;
import com.timgroup.statsd.StatsDClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DBSStatsdClient {
   private static final Logger LOG = LoggerFactory.getLogger(DBSStatsdClient.class);
   private static StatsDClient statsdClient;

   public static synchronized StatsDClient getInstance() {
      if (statsdClient == null) {
         DBSPrestoConfiguration DBSPrestoConfiguration = com.dbs.edsf.presto.authorization.policy.DBSPrestoConfiguration.getInstance();
         String statsdHost = DBSPrestoConfiguration.getVar(com.dbs.edsf.presto.authorization.policy.DBSPrestoConfiguration.DBSConfVars.DBS_PRESTO_PLUGIN_STATSD_HOST);
         String statsdPrefix = DBSPrestoConfiguration.getVar(com.dbs.edsf.presto.authorization.policy.DBSPrestoConfiguration.DBSConfVars.DBS_PRESTO_PLUGIN_STATSD_PREFIX);
         Integer statsdPort = Integer.valueOf(DBSPrestoConfiguration.getVar(com.dbs.edsf.presto.authorization.policy.DBSPrestoConfiguration.DBSConfVars.DBS_PRESTO_PLUGIN_STATSD_PORT));

         try {
            statsdClient = new NonBlockingStatsDClient(statsdPrefix, statsdHost, statsdPort);
         } catch (Exception var5) {
            LOG.warn("StatsD client could not be started", var5);
            statsdClient = new NoOpStatsDClient();
         }
      }

      return statsdClient;
   }
}
