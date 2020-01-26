package com.dbs.edsf.presto.authorization.ast;

import com.dbs.edsf.presto.authorization.DBSSystemAccessControl;
import com.dbs.edsf.presto.metrics.Metric;
import io.prestosql.spi.StatementRewriteContext;
import io.prestosql.spi.security.AccessDeniedException;
import io.prestosql.sql.parser.SqlParser;
import io.prestosql.sql.tree.Statement;

public interface QueryRewrite {
   @Metric(
      key = "query_modification.total_time",
      type = Metric.TYPE.TIMER
   )
   String rewrite(StatementRewriteContext var1, SqlParser var2, Statement var3, DBSSystemAccessControl var4) throws AccessDeniedException;
}
