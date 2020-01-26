package com.dbs.edsf.presto.authorization.policy;

import com.google.common.collect.Lists;
import java.io.File;
import java.net.URL;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DBSPrestoConfiguration extends Configuration {
   private static final Logger LOG = LoggerFactory.getLogger(DBSPrestoConfiguration.class);
   public static final String DBS_SITE_FILE_NAME = "DBS-presto-site.xml";
   public static final String DBS_SITE_FILE_PATH_KEY = "DBS.presto.configuration";
   public static final String DBS_SITE_FILE_DEFAULT = "/etc/DBS/pep/presto-ep/conf/DBS-presto-site.xml";
   private static DBSPrestoConfiguration DBSPrestoConfiguration;
   private static Map<String, String> conf = new HashMap(3);

   private DBSPrestoConfiguration() {
      this(true);
   }

   private DBSPrestoConfiguration(boolean addResource) {
      if (addResource) {
         ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
         if (classLoader == null) {
            classLoader = this.getClass().getClassLoader();
         }

         URL DBSConfigURL = classLoader.getResource("DBS-presto-site.xml");
         if (DBSConfigURL != null) {
            this.addResource(DBSConfigURL);
            LOG.info("Loaded {} from classpath", DBSConfigURL);
         } else {
            String path = (String)conf.getOrDefault("DBS.presto.configuration", "/etc/DBS/pep/presto-ep/conf/DBS-presto-site.xml");

            try {
               File siteFile = new File(path);
               DBSConfigURL = siteFile.toURI().toURL();
               if (DBSConfigURL != null) {
                  this.addResource(DBSConfigURL);
               } else {
                  LOG.error("Error loading resource: " + siteFile.getPath());
               }
            } catch (Exception var6) {
               LOG.error("Error loading resource:" + path, var6);
            }
         }
      }

   }

   public String get(String varName) {
      return this.get(varName, (String)null);
   }

   public String get(String varName, String defaultVal) {
      String retVal = super.get(varName);
      if (retVal == null) {
         retVal = defaultVal;
      }

      if (retVal == null) {
         retVal = DBSConfVars.getDefault(varName);
      }

      return retVal;
   }

   public String getDataDomain() {
      String varName = DBSConfVars.DBS_PRESTO_PLUGIN_PDP_DOMAIN.varName;
      String retVal = this.get(varName, DBSConfVars.getDefault(varName));
      return retVal;
   }

   public String getVar(DBSPrestoConfiguration.DBSConfVars confVar) {
      return this.get(confVar.getVar());
   }

   public List<String> getTrimmedVarList(DBSPrestoConfiguration.DBSConfVars confVar, String separatorChar) {
      List<String> varList = Lists.newArrayList();
      String val = this.getVar(confVar);
      if (!StringUtils.isEmpty(val)) {
         String[] tokens = this.getVar(confVar).split(separatorChar);
         String[] var6 = tokens;
         int var7 = tokens.length;

         for(int var8 = 0; var8 < var7; ++var8) {
            String token = var6[var8];
            varList.add(token.trim());
         }
      }

      return varList;
   }

   public void setVar(DBSPrestoConfiguration.DBSConfVars confVar, String value) {
      this.set(confVar.getVar(), value);
   }

   public boolean getBooleanVar(DBSPrestoConfiguration.DBSConfVars confVar) {
      return String.valueOf(true).equalsIgnoreCase(this.getVar(confVar));
   }

   public void setBooleanVar(DBSPrestoConfiguration.DBSConfVars confVar, boolean value) {
      this.setVar(confVar, String.valueOf(value));
   }

   public static synchronized DBSPrestoConfiguration getInstance() {
      if (DBSPrestoConfiguration == null) {
         DBSPrestoConfiguration = new DBSPrestoConfiguration(true);
      }

      return DBSPrestoConfiguration;
   }

   public DBSPrestoConfiguration.AuthenticationType getAuthType() {
      DBSPrestoConfiguration.AuthenticationType authType = AuthenticationType.fromValue(this.get(DBSConfVars.DBS_PRESTO_PLUGIN_AUTH_TYPE.getVar()));
      return authType;
   }

   public static void configure(Map<String, String> props) {
      conf.putAll(props);
   }

   public static enum DBSConfVars {
      DBS_PRESTO_PLUGIN_ENABLED("DBS.presto.plugin.enable", "true"),
      DBS_PRESTO_PLUGIN_AUTH_TYPE("DBS.presto.plugin.authentication.type", AuthenticationType.KERBEROS.toString()),
      DBS_PRESTO_PLUGIN_MOCK_POLICIES("DBS.presto.plugin.mock.policies", ""),
      DBS_PRESTO_PLUGIN_TESTMODE("DBS.presto.plugin.testmode", "false"),
      DBS_PRESTO_PLUGIN_PDP_ADDRESS("DBS.presto.plugin.pdp.address", (String)null),
      DBS_PRESTO_PLUGIN_CONFIG_ADDRESS("DBS.presto.plugin.config.address", (String)null),
      DBS_PRESTO_PLUGIN_PDP_HA_TAG("DBS.presto.plugin.pdp.tag", "*"),
      DBS_PRESTO_PLUGIN_PDP_PORT("DBS.presto.plugin.pdp.port", (String)null),
      DBS_PRESTO_PLUGIN_CATALOG_DOMAIN("DBS.presto.plugin.catalog.hive.domain", (String)null),
      DBS_PRESTO_PLUGIN_PDP_DOMAIN("DBS.presto.plugin.pdp.domain", (String)null),
      DBS_PRESTO_PLUGIN_CATALOGS_WHITELIST("DBS.presto.plugin.catalogs.whitelist", "system.runtime,system.metadata,system.jdbc"),
      DBS_PRESTO_PLUGIN_PDP_DOMAIN_TYPE("DBS.presto.plugin.pdp.domain.type", (String)null),
      DBS_PRESTO_PLUGIN_KAFKA_BROKER("DBS.kafka.metadata.broker.list", "localhost:9092"),
      DBS_PRESTO_PLUGIN_KAFKA_KEY_SERIALIZER_CLASS("DBS.kafka.key.serializer.class", "org.apache.kafka.common.serialization.StringSerializer"),
      DBS_PRESTO_PLUGIN_KAFKA_VALUE_SERIALIZER_CLASS("DBS.kafka.value.serializer.class", "org.apache.kafka.common.serialization.StringSerializer"),
      DBS_PRESTO_PLUGIN_TOPIC("DBS.kafka.topic", "ActivityMonitor"),
      DBS_PRESTO_PLUGIN_KAFKA_REQ_ACK("DBS.kafka.request.required.acks", "1"),
      DBS_PRESTO_PLUGIN_KAFKA_RECONNECT_BACKOFF("DBS.kafka.reconnect.backoff.ms", "100"),
      DBS_PRESTO_PLUGIN_KAFKA_MAX_SEND_BLOCK("DBS.kafka.message.send.max.block.ms", "100"),
      DBS_PRESTO_PLUGIN_KAFKA_REQUEST_TIMEOUT("DBS.kafka.request.timeout.ms", "100"),
      DBS_PRESTO_PLUGIN_KAFKA_MAX_RETRIES("DBS.kafka.message.send.max.retries", "5"),
      DBS_PRESTO_PLUGIN_KAFKA_RETRY_BACKOFF("DBS.kafka.retry.backoff.ms", "100"),
      DBS_PRESTO_PLUGIN_KAFKA_BATCH_SIZE("DBS.kafka.batch.num.messages", "10"),
      DBS_PRESTO_PLUGIN_AUDIT_ENABLED("DBS.presto.plugin.audit.enabled", "true"),
      DBS_PRESTO_PLUGIN_AUDIT_SKIP_ACTIONS("DBS.presto.plugin.audit.skip.actions", "USE"),
      DBS_PRESTO_PLUGIN_AUDIT_SKIP_USERS("DBS.presto.plugin.audit.skip.users", ""),
      DBS_PRESTO_PLUGIN_POLICY_ENFORCEMENT_ENABLED("DBS.presto.plugin.policy.enforcement.enabled", "true"),
      DBS_PRESTO_PLUGIN_EMBED_PDP("DBS.presto.plugin.embedPdp", "true"),
      DBS_PRESTO_PLUGIN_TCP_NODELAY("DBS.presto.plugin.nodelay", "true"),
      DBS_PRESTO_PLUGIN_USE_CONNECTION_POOL("DBS.presto.plugin.use.connection.pool", "false"),
      DBS_PRESTO_PLUGIN_CONNECTION_POOL_MAX_IDLE("DBS.presto.plugin.pool.max.idle", "100"),
      DBS_PRESTO_PLUGIN_CONNECTION_POOL_MIN_IDLE("DBS.presto.plugin.pool.min.idle", "5"),
      DBS_PRESTO_PLUGIN_CONNECTION_POOL_MAX_CONNECTIONS("DBS.presto.plugin.pool.max.connections", String.valueOf(Integer.MAX_VALUE)),
      DBS_PRESTO_PLUGIN_CONNECTION_POOL_GC_INTERVAL("DBS.presto.plugin.pool.gc.interval", "60000"),
      DBS_PRESTO_PLUGIN_ENFORCE_DELETE("DBS.presto.plugin.enforce.delete", "false"),
      DBS_PRESTO_PLUGIN_POLICY_REDEPOY_POLLING_INTERVAL("DBS.presto.plugin.policy.redeploy.polling.interval.secs", "300"),
      DBS_PRESTO_PLUGIN_IMPERSONATION_VALIDATION_ENABLED("DBS.presto.plugin.impersonation.validation.enabled", "true"),
      DBS_PRESTO_PLUGIN_OPTIMIZE_POLICY_LOOKUP("DBS.presto.plugin.optimize.policy.lookup", "true"),
      DBS_PRESTO_PLUGIN_QUERY_PARTITIONS_ENABLED("DBS.presto.plugin.query.partitions.enabled", "true"),
      DBS_PRESTO_PLUGIN_STATSD_PREFIX("DBS.presto.plugin.statsd.prefix", "DBS.presto"),
      DBS_PRESTO_PLUGIN_STATSD_HOST("DBS.presto.plugin.statsd.host", "127.0.0.1"),
      DBS_PRESTO_PLUGIN_STATSD_PORT("DBS.presto.plugin.statsd.port", "8125"),
      DBS_PRESTO_PLUGIN_CATALOGS_HS2_PARITY_LIST("DBS.presto.plugin.catalogs.hs2.parity.list", (String)null);

      private final String varName;
      private final String defaultVal;

      private DBSConfVars(String varName, String defaultVal) {
         this.varName = varName;
         this.defaultVal = defaultVal;
      }

      public String getVar() {
         return this.varName;
      }

      public String getDefault() {
         return this.defaultVal;
      }

      public static String getDefault(String varName) {
         DBSPrestoConfiguration.DBSConfVars[] var1 = values();
         int var2 = var1.length;

         for(int var3 = 0; var3 < var2; ++var3) {
            DBSPrestoConfiguration.DBSConfVars oneVar = var1[var3];
            if (oneVar.getVar().equalsIgnoreCase(varName)) {
               return oneVar.getDefault();
            }
         }

         return null;
      }
   }

   public static enum AuthenticationType {
      CERTIFICATE,
      KERBEROS,
      PASSWORD,
      JWT;

      public static DBSPrestoConfiguration.AuthenticationType fromValue(String name) {
         DBSPrestoConfiguration.AuthenticationType[] var1 = values();
         int var2 = var1.length;

         for(int var3 = 0; var3 < var2; ++var3) {
            DBSPrestoConfiguration.AuthenticationType v = var1[var3];
            if (v.toString().equalsIgnoreCase(name)) {
               return v;
            }
         }

         return KERBEROS;
      }
   }
}
