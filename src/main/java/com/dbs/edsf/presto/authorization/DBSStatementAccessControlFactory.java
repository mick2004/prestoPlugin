package com.dbs.edsf.presto.authorization;

import com.dbs.edsf.presto.authorization.policy.DBSPolicyHandler;
import com.dbs.edsf.presto.authorization.policy.DBSPrestoConfiguration;
import io.prestosql.spi.security.StatementAccessControl;
import io.prestosql.spi.security.StatementAccessControlFactory;
import java.util.Map;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DBSStatementAccessControlFactory implements StatementAccessControlFactory {
   private static final Logger LOG = LoggerFactory.getLogger(DBSStatementAccessControlFactory.class);
   private Optional<DBSPolicyHandler> DBSPolicyHandler = Optional.empty();

   public DBSStatementAccessControlFactory() {
   }

   public DBSStatementAccessControlFactory(Optional<DBSPolicyHandler> DBSPolicyHandler) {
      this.DBSPolicyHandler = DBSPolicyHandler;
   }

   public String getName() {
      return "DBSStatementAccessControl";
   }

   public StatementAccessControl create(Map<String, String> config) {
      LOG.info("Configuration in statement-access-control.properties:{}", config);
      DBSPrestoConfiguration.configure(config);
      return this.DBSPolicyHandler.isPresent() ? new DBSStatementAccessControl((DBSPolicyHandler)this.DBSPolicyHandler.get()) : new DBSStatementAccessControl();
   }
}
