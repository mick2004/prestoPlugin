package com.dbs.edsf.presto.authorization;

//import com.dbs.edsf.messages.DBSAuditEventLogger;
import com.dbs.edsf.presto.authorization.ast.DBSRowFilterColumnMaskRewrite;
import com.dbs.edsf.presto.authorization.ast.QueryRewrite;
import com.dbs.edsf.presto.authorization.policy.DBSAuthUtils;
import com.dbs.edsf.presto.authorization.policy.DBSPolicyHandler;
import com.dbs.edsf.presto.authorization.policy.DBSPrestoConfiguration;
import com.dbs.edsf.presto.authorization.policy.PolicyHandler;
import com.dbs.edsf.presto.metrics.DBSMetricsProxy;
import com.dbs.edsf.presto.metrics.DBSStatsdClient;
import io.prestosql.spi.StatementRewriteContext;
import io.prestosql.spi.security.AccessDeniedException;
import io.prestosql.spi.security.StatementAccessControl;
import io.prestosql.sql.parser.ParsingOptions;
import io.prestosql.sql.parser.SqlParser;
import io.prestosql.sql.parser.ParsingOptions.DecimalLiteralTreatment;
import io.prestosql.sql.tree.Execute;
import io.prestosql.sql.tree.Prepare;
import io.prestosql.sql.tree.Statement;
import io.prestosql.sql.tree.Use;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DBSStatementAccessControl implements StatementAccessControl {
   private static final Logger LOG = LoggerFactory.getLogger(DBSStatementAccessControl.class);
   private final QueryRewrite filterMaskRewriter;
   private final DBSSystemAccessControl DBSSystemAccessControl;
  // private final DBSAuditEventLogger auditEventLogger;
   private final SqlParser parser = new SqlParser();
   private DBSPrestoConfiguration DBSPrestoConfiguration = com.dbs.edsf.presto.authorization.policy.DBSPrestoConfiguration.getInstance();

   public DBSStatementAccessControl() {
      PolicyHandler DBSPolicyHandler = (PolicyHandler)DBSMetricsProxy.newInstance(new DBSPolicyHandler(), PolicyHandler.class);
      this.DBSSystemAccessControl = new DBSSystemAccessControl(DBSPolicyHandler);
      //this.auditEventLogger = new DBSAuditEventLogger();
      this.filterMaskRewriter = (QueryRewrite)DBSMetricsProxy.newInstance(new DBSRowFilterColumnMaskRewrite(), QueryRewrite.class);
   }

   public DBSStatementAccessControl(DBSPolicyHandler DBSPolicyHandler) {
      this.DBSSystemAccessControl = new DBSSystemAccessControl((PolicyHandler)DBSMetricsProxy.newInstance(DBSPolicyHandler, PolicyHandler.class));
      //this.auditEventLogger = new DBSAuditEventLogger();
      this.filterMaskRewriter = (QueryRewrite)DBSMetricsProxy.newInstance(new DBSRowFilterColumnMaskRewrite(), QueryRewrite.class);
   }

   public String getModifiedQuery(StatementRewriteContext session, String query) throws AccessDeniedException {
      String modifiedQuery;
      if (LOG.isInfoEnabled()) {
         LOG.info("StatementRewriteContext: {}", session.toString());
         modifiedQuery = String.format("DBSStatementAccessControl.getModifiedQuery() entering: user=%s,principal=%s,query=%s,catalog=%s,schema=%s", session.getIdentity().getUser(), session.getIdentity().getPrincipal(), query, session.getCatalog().orElse(""), session.getSchema().orElse(""));
         LOG.info(modifiedQuery);
      }

      modifiedQuery = query;
      String action = "";
      String userName = DBSAuthUtils.getUserName(session.getIdentity());

      try {
         Statement stmt = this.parser.createStatement(query, new ParsingOptions(DecimalLiteralTreatment.AS_DECIMAL));
         action = DBSAuthUtils.getAction(stmt);
         LOG.debug("Input query statement: {}", stmt);
         if (stmt instanceof Prepare || stmt instanceof Execute || stmt instanceof Use) {
            return query;
         }

         try {
            this.DBSSystemAccessControl.checkCanSetUser(session.getIdentity().getPrincipal(), session.getIdentity().getUser(), session.getRemoteUserAddress(), session.getCatalog(), session.getSchema());
            modifiedQuery = this.filterMaskRewriter.rewrite(session, this.parser, stmt, this.DBSSystemAccessControl);
            DBSStatsdClient.getInstance().incrementCounter("queries.handled");
         } catch (UnsupportedOperationException var8) {
            LOG.warn("Query modification not done for {}: {}", query, var8.getMessage());
         }
      } catch (AccessDeniedException var9) {
         //this.auditEventLogger.logEvent("DBS", userName, query, action, this.DBSPrestoConfiguration.getDataDomain(), false, "", (String)session.getRemoteUserAddress().orElse(""));
         String formattedStr = String.format("DBSStatementAccessControl.getModifiedQuery() exiting: user=%s,catalog=%s,schema=%s,message=%s", userName, session.getCatalog().orElse(""), session.getSchema().orElse(""), var9.getMessage());
         LOG.info(formattedStr);
         throw var9;
      }

      //this.auditEventLogger.logEvent("DBS", userName, query, action, this.DBSPrestoConfiguration.getDataDomain(), true, modifiedQuery, (String)session.getRemoteUserAddress().orElse(""));
      if (LOG.isInfoEnabled()) {
         String formattedStr = String.format("DBSStatementAccessControl.getModifiedQuery() exiting: user=%s,catalog=%s,schema=%s,modifiedQuery=%s", userName, session.getCatalog().orElse(""), session.getSchema().orElse(""), modifiedQuery);
         LOG.info(formattedStr);
      }

      modifiedQuery = "Select id,name,to_hex(md5(to_utf8(cast(name as varchar)))) as hex from userdb.table2 order by id";

      modifiedQuery=modifiedQuery + " limit 2";

      return modifiedQuery;
   }
}
