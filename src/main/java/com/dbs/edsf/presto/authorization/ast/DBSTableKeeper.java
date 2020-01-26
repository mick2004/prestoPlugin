package com.dbs.edsf.presto.authorization.ast;

import com.dbs.edsf.presto.authorization.policy.DBSAuthUtils;
import com.dbs.edsf.presto.authorization.policy.DBSPrestoConfiguration;
import com.dbs.edsf.presto.authorization.policy.PolicyHandler;
import io.prestosql.spi.StatementRewriteContext;
import io.prestosql.spi.connector.CatalogSchemaTableName;
import io.prestosql.spi.security.AccessDeniedException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DBSTableKeeper {
   private static final Logger LOG = LoggerFactory.getLogger(DBSTableKeeper.class);
   private AtomicInteger tableCounter = new AtomicInteger(0);
   private PolicyHandler DBSPolicyHandler;
   private StatementRewriteContext session;
   private static final String DOUBLE_UNDERSCORE = "__";
   private static final String UNION = " UNION ";
   private static final String TABLE_NAME_PREFIX = "__DBS_t";
   private static final Pattern UNION_SPLIT_PATTERN = Pattern.compile("(?i)UNION");
   private static final Pattern GENERATED_VIEW_PATTERN = Pattern.compile("(?i)select[\\s]+DBS[\\s]+from[\\s]+__DBS_t[0-9]+");
   private boolean optimizePolicyLookUp;
   private Map<CatalogSchemaTableName, DBSTableKeeper.DBSTableView> actualTableMap = new HashMap();
   private Map<String, DBSTableKeeper.DBSTableView> generatedTableMap = new HashMap();

   public DBSTableKeeper(StatementRewriteContext session, PolicyHandler DBSPolicyHandler) {
      this.DBSPolicyHandler = DBSPolicyHandler;
      this.session = session;
      this.optimizePolicyLookUp = DBSPrestoConfiguration.getInstance().getBooleanVar(DBSPrestoConfiguration.DBSConfVars.DBS_PRESTO_PLUGIN_OPTIMIZE_POLICY_LOOKUP);
   }

   public String getPlaceholder(CatalogSchemaTableName catalogSchemaTableName) {
      DBSTableKeeper.DBSTableView DBSTableName;
      if (this.actualTableMap.get(catalogSchemaTableName) == null) {
         DBSTableName = new DBSTableKeeper.DBSTableView(catalogSchemaTableName, this.tableCounter.incrementAndGet());
         this.actualTableMap.put(catalogSchemaTableName, DBSTableName);
         this.generatedTableMap.put(DBSTableName.getGeneratedTableName(), DBSTableName);
      }

      DBSTableName = (DBSTableKeeper.DBSTableView)this.actualTableMap.get(catalogSchemaTableName);
      return DBSTableName.getGeneratedView();
   }

   public String replacePlaceHolders(String query) {
      if (this.actualTableMap.size() == 0) {
         return query;
      } else {
         LOG.debug("Query with place holders: {}", query);
         this.frameUnionQueryAndExecute();
         Matcher matcher = GENERATED_VIEW_PATTERN.matcher(query);
         StringBuilder modifiedBuffer = new StringBuilder();

         int index;
         for(index = 0; matcher.find(); index = matcher.end()) {
            String generatedInlineView = matcher.group();
            String[] querySplits = generatedInlineView.split("__");
            DBSTableKeeper.DBSTableView tableView = (DBSTableKeeper.DBSTableView)this.generatedTableMap.get("__" + querySplits[querySplits.length - 1]);
            modifiedBuffer.append(query.substring(index, matcher.start()));
            modifiedBuffer.append(tableView.getInlineView());
         }

         modifiedBuffer.append(query.substring(index, query.length()));
         LOG.debug("Query after place holder replacement: {}", modifiedBuffer.toString());
         return modifiedBuffer.toString();
      }
   }

   private void frameUnionQueryAndExecute() {
      ArrayList actualTableNames = new ArrayList(this.actualTableMap.keySet());

      try {
         StringBuilder modifiedQuery = new StringBuilder();
         boolean optimize = this.optimizePolicyLookUp && this.actualTableMap.size() > 1;
         if (optimize) {
            StringBuilder sBuilder = new StringBuilder();
            actualTableNames.forEach((tableNamex) -> {
               DBSTableKeeper.DBSTableView DBSTableView = (DBSTableKeeper.DBSTableView)this.actualTableMap.get(tableNamex);
               sBuilder.append("select * ").append(" from ").append(DBSTableView.getHiveParityTableName());
               if (!tableNamex.equals(actualTableNames.get(actualTableNames.size() - 1))) {
                  sBuilder.append(" UNION ");
               }

            });
            String query = this.DBSPolicyHandler.getTransformedQuery(this.session.getIdentity(), sBuilder.toString(), this.session.getCatalog(), this.session.getSchema());
            modifiedQuery.append(query);
         } else {
            actualTableNames.forEach((tableNamex) -> {
               DBSTableKeeper.DBSTableView DBSTableView = (DBSTableKeeper.DBSTableView)this.actualTableMap.get(tableNamex);
               modifiedQuery.append(this.DBSPolicyHandler.getInlineView(this.session.getIdentity(), DBSTableView.getHiveParityTableName(), Optional.empty()));
               if (!tableNamex.equals(actualTableNames.get(actualTableNames.size() - 1))) {
                  modifiedQuery.append(" UNION ");
               }

            });
         }

         String[] views = UNION_SPLIT_PATTERN.split(modifiedQuery);

         for(int i = 0; i < views.length; ++i) {
            String view = views[i].trim();
            ((DBSTableKeeper.DBSTableView)this.actualTableMap.get(actualTableNames.get(i))).setInlineView(view);
         }

      } catch (AccessDeniedException var9) {
         LOG.info(var9.getMessage(), var9);
         String message = DBSAuthUtils.formatExceptionMessage(var9);
         Iterator tableNameIterator = actualTableNames.iterator();

         while(tableNameIterator.hasNext()) {
            CatalogSchemaTableName tableName = (CatalogSchemaTableName)tableNameIterator.next();
            DBSTableKeeper.DBSTableView DBSTableView = (DBSTableKeeper.DBSTableView)this.actualTableMap.get(tableName);
            String hiveParityString = DBSAuthUtils.toString(DBSTableView.getHiveParityTableName());
            String actualTableString = DBSAuthUtils.toString(DBSTableView.getActualTableName());
            if (!hiveParityString.equalsIgnoreCase(actualTableString)) {
               message = message.replaceAll("(?i)" + hiveParityString, actualTableString);
            }
         }

         throw new AccessDeniedException(message);
      }
   }

   public class DBSTableView {
      private final CatalogSchemaTableName actualTableName;
      private final CatalogSchemaTableName hiveParityTableName;
      private final String generatedTableName;
      private final String generatedView;
      private String inlineView;

      public DBSTableView(CatalogSchemaTableName actualTableName, int counter) {
         this.actualTableName = actualTableName;
         this.hiveParityTableName = DBSAuthUtils.getHiveParityCatalog(actualTableName);
         this.generatedTableName = "__DBS_t" + counter;
         this.generatedView = String.format("select %s from %s", "DBS", this.generatedTableName);
      }

      public String getGeneratedView() {
         return this.generatedView;
      }

      public CatalogSchemaTableName getActualTableName() {
         return this.actualTableName;
      }

      public CatalogSchemaTableName getHiveParityTableName() {
         return this.hiveParityTableName;
      }

      public String getGeneratedTableName() {
         return this.generatedTableName;
      }

      public String getInlineView() {
         return this.inlineView;
      }

      public void setInlineView(String inlineView) {
         String hiveParityString = Pattern.quote(DBSAuthUtils.toString(this.hiveParityTableName));
         String actualTableString = Matcher.quoteReplacement(DBSAuthUtils.formatToStringWithDelimiters(this.actualTableName));
         this.inlineView = inlineView.replaceAll("(?i)" + hiveParityString, actualTableString);
      }
   }
}
