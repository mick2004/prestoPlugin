package com.dbs.edsf.presto.authorization.policy;

import com.dbs.edsf.presto.DBSclient.DBSPolicyEngineConnector;
//import com.dbs.edsf.tcpclient.PEException;
//import com.dbs.edsf.tcpclient.PolicyEngineClientConfiguration;
//import com.dbs.edsf.tcpclient.TCPClient;
import com.google.common.base.Preconditions;
import io.prestosql.spi.connector.CatalogSchemaTableName;
import io.prestosql.spi.security.AccessDeniedException;
import io.prestosql.spi.security.Identity;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DBSPolicyHandler implements PolicyHandler {
   private static final Logger LOG = LoggerFactory.getLogger(DBSPolicyHandler.class);
   public static final String ARC_SEC_TABLE_NAME = "ARCSecurity";
   public static final String ARC_ACC_TABLE_NAME = "ARCAccessDenied";
   public static final String IMPERSONATION_DO_AS = "DoAs";
   public static final String DENY_MESSAGE = "User %s is not authorized by auth plugin";
   public static final String ALL_TABLES_NOT_ACCESSIBLE = ": all or some tables are not accessible";
   public static final String IMPERSONATION_VALIDATION_FAILURE = "Impersonation check failed as %s";
   public static final String POLICY_ENGINE_CONNECTION_FAILURE = "Failed to connect to policy engine, %s";
   public static final String USER_NOT_AUTHORIZED = "User not authorized";
   public static final String NO_KERBEROS_PRINCIPAL = "No kerberos principal defined for trusted/functional user";
   public static final String PARTITIONS_TABLE_QUERY_MODIFICATION = "partitions table %s not supported for policy enforcement";
   public static final String NULL_IP = "0.0.0.0";
   private DBSPolicyEngineConnector connector = null;
   private String policyEngineAddress;
   private int policyEnginePortNumber;
   private String policyConfigAddress;
   private String policyHaTag;
   private boolean isPolicyEngineHA = false;
   private DBSPrestoConfiguration prestoConfiguration = DBSPrestoConfiguration.getInstance();
   //private PolicyEngineClientConfiguration policyEngineClientConfiguration;
   //private DBSTcpClientPoolFactory poolFactory;
   //private GenericObjectPool<TCPClient> pool;
   private final boolean setTcpNoDelay;
   private boolean useConnectionPooling = false;

   public DBSPolicyHandler() {
      this.policyConfigAddress = this.prestoConfiguration.getVar(DBSPrestoConfiguration.DBSConfVars.DBS_PRESTO_PLUGIN_CONFIG_ADDRESS);
      if (this.policyConfigAddress == null) {
         LOG.warn(DBSPrestoConfiguration.DBSConfVars.DBS_PRESTO_PLUGIN_CONFIG_ADDRESS.getVar() + " not configured. Policy engine HA will not be supported.");
         this.policyEngineAddress = this.prestoConfiguration.getVar(DBSPrestoConfiguration.DBSConfVars.DBS_PRESTO_PLUGIN_PDP_ADDRESS);
         String policyEnginePortStr = this.prestoConfiguration.getVar(DBSPrestoConfiguration.DBSConfVars.DBS_PRESTO_PLUGIN_PDP_PORT);
         Preconditions.checkNotNull(policyEnginePortStr);
         this.policyEnginePortNumber = Integer.valueOf(policyEnginePortStr);
      } else {
         this.isPolicyEngineHA = true;
         this.policyHaTag = this.prestoConfiguration.getVar(DBSPrestoConfiguration.DBSConfVars.DBS_PRESTO_PLUGIN_PDP_HA_TAG);
      }

      this.setTcpNoDelay = "true".equalsIgnoreCase(this.prestoConfiguration.getVar(DBSPrestoConfiguration.DBSConfVars.DBS_PRESTO_PLUGIN_TCP_NODELAY));
      //this.poolFactory = new DBSTcpClientPoolFactory(this.prestoConfiguration, this.policyConfigAddress, this.isPolicyEngineHA, this.policyHaTag, this.policyEngineAddress, this.policyEnginePortNumber, this.setTcpNoDelay);
      this.useConnectionPooling = "true".equalsIgnoreCase(this.prestoConfiguration.getVar(DBSPrestoConfiguration.DBSConfVars.DBS_PRESTO_PLUGIN_USE_CONNECTION_POOL));
      if (this.useConnectionPooling) {
         //this.pool = new GenericObjectPool(this.poolFactory, this.readConfiguration(this.prestoConfiguration), new AbandonedConfig());
      }

      this.connector = this.getDriver();
   }

   private GenericObjectPoolConfig readConfiguration(DBSPrestoConfiguration DBSConfig) {
      GenericObjectPoolConfig poolConfig = new GenericObjectPoolConfig();
      poolConfig.setMaxIdle(Integer.valueOf(DBSConfig.getVar(DBSPrestoConfiguration.DBSConfVars.DBS_PRESTO_PLUGIN_CONNECTION_POOL_MAX_IDLE)));
      poolConfig.setMaxTotal(Integer.valueOf(DBSConfig.getVar(DBSPrestoConfiguration.DBSConfVars.DBS_PRESTO_PLUGIN_CONNECTION_POOL_MAX_CONNECTIONS)));
      poolConfig.setMinIdle(Integer.valueOf(DBSConfig.getVar(DBSPrestoConfiguration.DBSConfVars.DBS_PRESTO_PLUGIN_CONNECTION_POOL_MIN_IDLE)));
      poolConfig.setTimeBetweenEvictionRunsMillis((long)Integer.valueOf(DBSConfig.getVar(DBSPrestoConfiguration.DBSConfVars.DBS_PRESTO_PLUGIN_CONNECTION_POOL_GC_INTERVAL)));
      return poolConfig;
   }

//   public TCPClient getConnection() throws RuntimeException {
//      try {
//         if (this.useConnectionPooling) {
//            LOG.debug("TcpClient connections in the pool#{}: Active - {}, Idle - {}", new Object[]{this.pool.hashCode(), this.pool.getNumActive(), this.pool.getNumIdle()});
//            return (TCPClient)this.pool.borrowObject();
//         } else {
//            return this.poolFactory.getNewConnection();
//         }
//      } catch (Exception var2) {
//         throw new RuntimeException("Error getting connection to PDP: ", var2);
//      }
//   }

//   public void closeConnection(TCPClient DBSEngineTcpClient) throws RuntimeException {
//      if (DBSEngineTcpClient != null) {
//         if (this.useConnectionPooling) {
//            this.pool.returnObject(DBSEngineTcpClient);
//         } else {
//            DBSEngineTcpClient.closeSession();
//         }
//
//      }
//   }

   protected DBSPolicyEngineConnector getDriver() {
      return new DBSPolicyEngineConnector();
   }

   private boolean isTableWhitelisted(CatalogSchemaTableName queryTableOption) {
      String queryTable = DBSAuthUtils.toString(queryTableOption);
      List<String> whiteListTables = this.prestoConfiguration.getTrimmedVarList(DBSPrestoConfiguration.DBSConfVars.DBS_PRESTO_PLUGIN_CATALOGS_WHITELIST, ",");
      return whiteListTables.stream().anyMatch((whileListedTable) -> {
         return DBSAuthUtils.startsWith(queryTable, whileListedTable, true);
      });
   }

   private boolean isPartitionsTable(CatalogSchemaTableName queryTableOption) {
      String queryTable = DBSAuthUtils.toString(queryTableOption);
      boolean allowPartitionsQuery = this.prestoConfiguration.getBooleanVar(DBSPrestoConfiguration.DBSConfVars.DBS_PRESTO_PLUGIN_QUERY_PARTITIONS_ENABLED);
      return allowPartitionsQuery && queryTable.toLowerCase().endsWith("$partitions");
   }

   private String getUserNameForTableAccess(Identity identity, Optional<List<CatalogSchemaTableName>> tableNames) {
      DBSPrestoConfiguration.AuthenticationType authType = this.prestoConfiguration.getAuthType();
      boolean allTablesAreWhitelisted = (Boolean)tableNames.map((tables) -> {
         return tables.stream().allMatch((table) -> {
            return this.isTableWhitelisted(table);
         });
      }).orElse(false);
      if (!allTablesAreWhitelisted) {
         switch(authType) {
         case KERBEROS:
            identity.getPrincipal().orElse(() -> {
               //return new AccessDeniedException("No kerberos principal defined for trusted/functional user");
               return "abhisheksingh";
            });
         }
      }

      return DBSAuthUtils.getUserName(identity);
   }

   public void validateTrustedUser(String userName, Optional<String> remoteUserAddress, Optional<String> catalogName, Optional<String> schema) {
//      String userIp = (String)remoteUserAddress.orElse("0.0.0.0");
//      String DBSDataDomain = this.prestoConfiguration.getDataDomain();
//      TCPClient connection = null;
//
//      try {
//         connection = this.getConnection();
//         String response = this.connector.getTrustedUserDetails(connection, userName);
//         LOG.debug("validateTrustedUser() response from policy engine for user {}: {}", userName, response);
//         String[] parts = response.split("[|]");
//
//         for(int i = 0; i < parts.length; ++i) {
//            String entity = parts[i].trim();
//            switch(i) {
//            case 1:
//               if (!entity.equalsIgnoreCase(userName)) {
//                  throw new AccessDeniedException(String.format("Impersonation check failed as %s", "user name " + userName + " does not match"));
//               }
//               break;
//            case 2:
//               if (!DBSAuthUtils.checkIpInCIDR(entity, userIp)) {
//                  throw new AccessDeniedException(String.format("Impersonation check failed as %s", "remote user address " + userIp + " not whitelisted"));
//               }
//               break;
//            case 3:
//               if (!entity.equalsIgnoreCase("DoAs")) {
//                  throw new AccessDeniedException(String.format("Impersonation check failed as %s", "DoAs impersonation is only supported"));
//               }
//            }
//         }
//      } catch (IOException var16) {
//         if (var16.getMessage().contains("User not authorized")) {
//            LOG.warn("User not authorized", var16);
//            throw new AccessDeniedException(var16.getMessage());
//         }
//
//         String msg = String.format("Failed to connect to policy engine, %s", var16.getMessage());
//         LOG.error(msg, var16);
//         throw new RuntimeException(msg, var16);
//      } catch (PEException var17) {
//         LOG.warn(var17.getMessage(), var17);
//         throw new AccessDeniedException(var17.getMessage());
//      } finally {
//         this.closeConnection(connection);
//      }

   }

   public void checkAccess(Identity identity, String catalog, String schema, String query) throws AccessDeniedException {
//      String DBSDataDomain = this.prestoConfiguration.getDataDomain();
//      String user = this.getUserNameForTableAccess(identity, Optional.empty());
//      TCPClient connection = null;
//
//      try {
//         LOG.debug("input query:user: {}, query: {}", user, query);
//         connection = this.getConnection();
//         String formattedQuery = DBSAuthUtils.formatSql(query);
//         String safeSqlStmt = this.connector.getSafeSQL(connection, user, DBSDataDomain, catalog, schema, formattedQuery);
//         LOG.debug("checkAccess() response from policy engine: {}", safeSqlStmt);
//         if (safeSqlStmt.contains("ARCSecurity") || safeSqlStmt.contains("ARCAccessDenied")) {
//            throw new AccessDeniedException(String.format("User %s is not authorized by auth plugin", user));
//         }
//      } catch (IOException var15) {
//         if (var15.getMessage().contains("User not authorized")) {
//            LOG.warn("User not authorized", var15);
//            throw new AccessDeniedException(var15.getMessage());
//         }
//
//         String msg = String.format("Failed to connect to policy engine, %s", var15.getMessage());
//         LOG.error(msg, var15);
//         throw new RuntimeException(msg, var15);
//      } catch (PEException var16) {
//         LOG.warn(var16.getMessage(), var16);
//         throw new AccessDeniedException(var16.getMessage());
//      } finally {
//         this.closeConnection(connection);
//      }

   }

   public String getInlineView(Identity identity, CatalogSchemaTableName tableName, Optional<Set<String>> columns) throws AccessDeniedException {
      String DBSDataDomain = this.prestoConfiguration.getDataDomain();
      String catalog = tableName.getCatalogName();
      String schema = tableName.getSchemaTableName().getSchemaName();
      String user = this.getUserNameForTableAccess(identity, Optional.of(Arrays.asList(tableName)));
//      TCPClient connection = null;
//
     String var20 = "APS";
//      try {
//         String formattedQuery;
//         try {
//            LOG.debug("getInlineView() entered:user: {}, datadomain: {}, table: {}", new Object[]{user, DBSDataDomain, tableName});
//            connection = this.getConnection();
//            String query = "select " + this.columnsToString(columns) + " from " + tableName;
//            formattedQuery = DBSAuthUtils.formatSql(query);
//            String safeSqlStmt;
//            if (this.isPartitionsTable(tableName)) {
//               safeSqlStmt = query;
//            } else if (this.isTableWhitelisted(tableName)) {
//               safeSqlStmt = query;
//            } else {
//               safeSqlStmt = this.connector.getSafeSQL(connection, user, DBSDataDomain, catalog, schema, formattedQuery);
//               LOG.debug("getInlineView() response from policy engine: {}", safeSqlStmt);
//               if (safeSqlStmt.contains("ARCSecurity") || safeSqlStmt.contains("ARCAccessDenied")) {
//                  AccessDeniedException.denySelectTable(tableName.toString(), String.format("User %s is not authorized by auth plugin", user));
//               }
//
//               int index = safeSqlStmt.indexOf("|");
//               if (index != -1) {
//                  safeSqlStmt = safeSqlStmt.substring(index + 1, safeSqlStmt.length());
//               }
//            }
//
//            LOG.debug("getInlineView() exiting: {}", safeSqlStmt);
//            var20 = safeSqlStmt;
//         } catch (IOException var17) {
//            if (var17.getMessage().contains("User not authorized")) {
//               LOG.warn("User not authorized", var17);
//               throw new AccessDeniedException(var17.getMessage());
//            }
//
//            formattedQuery = String.format("Failed to connect to policy engine, %s", var17.getMessage());
//            LOG.error(formattedQuery, var17);
//            throw new RuntimeException(formattedQuery, var17);
//         } catch (PEException var18) {
//            LOG.warn(var18.getMessage(), var18);
//            throw new AccessDeniedException(var18.getMessage());
//         }
//      } finally {
//         this.closeConnection(connection);
//      }

      return var20;
   }

   public String getTransformedQuery(Identity identity, String query, Optional<String> catalogName, Optional<String> schemaName) throws AccessDeniedException {
      String DBSDataDomain = this.prestoConfiguration.getDataDomain();
      String catalog = (String)catalogName.orElse("");
      String schema = (String)schemaName.orElse("");
      //TCPClient connection = null;

      String var13="APS";
//      try {
//         String msg;
//         try {
//            String formattedQuery = DBSAuthUtils.formatSql(query);
//            msg = this.getUserNameForTableAccess(identity, Optional.empty());
//            LOG.debug("getTransformedQuery() entered:user: {}, datadomain: {}, query:{}", new Object[]{msg, DBSDataDomain, query});
//            connection = this.getConnection();
//            String safeSqlStmt = this.connector.getSafeSQL(connection, msg, DBSDataDomain, catalog, schema, formattedQuery);
//            LOG.debug("getTransformedQuery() response from policy engine: {}", safeSqlStmt);
//            if (safeSqlStmt.contains("ARCSecurity") || safeSqlStmt.contains("ARCAccessDenied")) {
//               AccessDeniedException.denySelectTable(": all or some tables are not accessible", String.format("User %s is not authorized by auth plugin", msg));
//            }
//
//            int index = safeSqlStmt.indexOf("|");
//            if (index != -1) {
//               safeSqlStmt = safeSqlStmt.substring(index + 1, safeSqlStmt.length());
//            }
//
//            LOG.debug("getTransformedQuery() exiting: {}", safeSqlStmt);
//            var13 = safeSqlStmt;
//         } catch (IOException var18) {
//            if (var18.getMessage().contains("User not authorized")) {
//               LOG.warn("User not authorized", var18);
//               throw new AccessDeniedException(var18.getMessage());
//            }
//
//            msg = String.format("Failed to connect to policy engine, %s", var18.getMessage());
//            LOG.error(msg, var18);
//            throw new RuntimeException(msg, var18);
//         } catch (PEException var19) {
//            LOG.warn(var19.getMessage(), var19);
//            throw new AccessDeniedException(var19.getMessage());
//         }
//      } finally {
//         this.closeConnection(connection);
//      }

      return var13;
   }

   private String columnsToString(Optional<Set<String>> columns) {
      StringBuilder sBuilder = new StringBuilder();
      if (columns.isPresent()) {
         Iterator iterator = ((Set)columns.get()).iterator();

         while(iterator.hasNext()) {
            String column = (String)iterator.next();
            sBuilder.append(column);
            if (iterator.hasNext()) {
               sBuilder.append(",");
            }
         }
      }

      return sBuilder.length() > 0 ? sBuilder.toString() : "*";
   }
}
