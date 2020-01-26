package com.dbs.edsf.presto.authorization;

import com.dbs.edsf.presto.authorization.policy.DBSPolicyHandler;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import io.prestosql.spi.Plugin;
import io.prestosql.spi.security.StatementAccessControlFactory;
import java.util.Objects;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DBSPrestoPlugin implements Plugin {
   private static final Logger LOG = LoggerFactory.getLogger(DBSPrestoPlugin.class);
   Optional<DBSPolicyHandler> DBSPolicyHandler = Optional.empty();

   public DBSPrestoPlugin() {
   }

   @VisibleForTesting
   public DBSPrestoPlugin(Optional<DBSPolicyHandler> DBSPolicyHandler) {
      this.DBSPolicyHandler = (Optional)Objects.requireNonNull(DBSPolicyHandler, "DBSPolicyHandler is null");
   }

   public Iterable<StatementAccessControlFactory> getStatementAccessControlFactories() {
      LOG.info("Entered DBSPrestoPlugin");
      return ImmutableList.of(new DBSStatementAccessControlFactory(this.DBSPolicyHandler));
   }
}
