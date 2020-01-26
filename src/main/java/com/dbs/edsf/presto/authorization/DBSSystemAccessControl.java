package com.dbs.edsf.presto.authorization;

import com.dbs.edsf.presto.authorization.policy.DBSAuthUtils;
import com.dbs.edsf.presto.authorization.policy.DBSDdlOperations;
import com.dbs.edsf.presto.authorization.policy.DBSPrestoConfiguration;
import com.dbs.edsf.presto.authorization.policy.PolicyHandler;
import io.prestosql.spi.connector.CatalogSchemaName;
import io.prestosql.spi.connector.CatalogSchemaTableName;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.spi.security.*;

import java.security.Principal;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DBSSystemAccessControl implements SystemAccessControl {
   private static final Logger LOG = LoggerFactory.getLogger(DBSSystemAccessControl.class);
   private PolicyHandler policyHandler;
   private Optional<List<String>> hiveParityCatalogs = Optional.empty();

   public DBSSystemAccessControl(PolicyHandler policyHandler) {
      this.policyHandler = policyHandler;
   }

   public PolicyHandler getPolicyHandler() {
      return this.policyHandler;
   }

   public void checkCanSetUser(Optional<Principal> principalOptional, String user, Optional<String> remoteUserAddress, Optional<String> catalogName, Optional<String> schema) {
      principalOptional.ifPresent((principal) -> {
         boolean validateImpersonation = DBSPrestoConfiguration.getInstance().getBooleanVar(DBSPrestoConfiguration.DBSConfVars.DBS_PRESTO_PLUGIN_IMPERSONATION_VALIDATION_ENABLED);
         if (validateImpersonation) {
            String principalName = DBSAuthUtils.getFirstPartInUserName(principal.getName());
            String userName = DBSAuthUtils.getFirstPartInUserName(user);
            if (!principalName.equals(userName)) {
               Optional<String> catalog = catalogName;
               if (catalogName.isPresent() && DBSAuthUtils.isCatalogInHiveParityList((String)catalogName.get())) {
                  catalog = schema;
               }

               this.policyHandler.validateTrustedUser(DBSAuthUtils.getFirstPartInUserName(principal.getName()), remoteUserAddress, catalog, schema);
            }
         }

      });
   }

   @Override
   public void checkCanAccessWebResource(Principal principal, String resource, String httpMethod) {

   }

   public void  checkCanSetUser(Optional<AuthenticatedUser> principal, String userName) {
   }

   @Override
   public void checkCanSetSystemSessionProperty(SystemSecurityContext context, String propertyName) {

   }

   public void checkCanSetSystemSessionProperty(Identity identity, String propertyName) {
   }

   public void checkCanAccessCatalog(Identity identity, String catalogName) {
   }

   public Set<String> filterCatalogs(Identity identity, Set<String> catalogs) {
      return catalogs;
   }

   public void checkCanCreateSchema(Identity identity, CatalogSchemaName catalogSchemaName) {
      CatalogSchemaName schema = DBSAuthUtils.getHiveParitySchema(catalogSchemaName);
      String query = DBSDdlOperations.CREATE_SCHEMA.getQuery(DBSAuthUtils.toString(schema));

      try {
         this.policyHandler.checkAccess(identity, schema.getCatalogName(), schema.getSchemaName(), query);
      } catch (AccessDeniedException var6) {
         AccessDeniedException.denyCreateSchema(DBSAuthUtils.toString(catalogSchemaName), DBSAuthUtils.formatExceptionMessage(var6));
      }

   }

   public void checkCanDropSchema(Identity identity, CatalogSchemaName catalogSchemaName) {
      CatalogSchemaName schema = DBSAuthUtils.getHiveParitySchema(catalogSchemaName);
      String query = DBSDdlOperations.DROP_SCHEMA.getQuery(DBSAuthUtils.toString(schema));

      try {
         this.policyHandler.checkAccess(identity, schema.getCatalogName(), schema.getSchemaName(), query);
      } catch (AccessDeniedException var6) {
         AccessDeniedException.denyDropSchema(DBSAuthUtils.toString(catalogSchemaName), DBSAuthUtils.formatExceptionMessage(var6));
      }

   }

   public void checkCanRenameSchema(Identity identity, CatalogSchemaName catalogSchemaName, String newSchemaName) {
      CatalogSchemaName schema = DBSAuthUtils.getHiveParitySchema(catalogSchemaName);
      String query = DBSDdlOperations.ALTER_SCHEMA.getQuery(DBSAuthUtils.toString(schema));

      try {
         this.policyHandler.checkAccess(identity, schema.getCatalogName(), schema.getSchemaName(), query);
      } catch (AccessDeniedException var7) {
         AccessDeniedException.denyRenameSchema(DBSAuthUtils.toString(catalogSchemaName), newSchemaName, DBSAuthUtils.formatExceptionMessage(var7));
      }

   }

   public void checkCanShowSchemas(Identity identity, String catalogName) {
   }

   public Set<String> filterSchemas(Identity identity, String catalogName, Set<String> schemaNames) {
      return schemaNames;
   }

   public void checkCanCreateTable(Identity identity, CatalogSchemaTableName catalogSchemaTableName) {
      CatalogSchemaTableName table = DBSAuthUtils.getHiveParityCatalog(catalogSchemaTableName);
      String query = DBSDdlOperations.CREATE_TABLE.getQuery(DBSAuthUtils.toString(table));

      try {
         this.policyHandler.checkAccess(identity, table.getCatalogName(), table.getSchemaTableName().getSchemaName(), query);
      } catch (AccessDeniedException var6) {
         AccessDeniedException.denyCreateTable(DBSAuthUtils.toString(catalogSchemaTableName), DBSAuthUtils.formatExceptionMessage(var6));
      }

   }

   public void checkCanDropTable(Identity identity, CatalogSchemaTableName catalogSchemaTableName) {
      CatalogSchemaTableName table = DBSAuthUtils.getHiveParityCatalog(catalogSchemaTableName);
      String query = DBSDdlOperations.DROP_TABLE.getQuery(DBSAuthUtils.toString(table));

      try {
         this.policyHandler.checkAccess(identity, table.getCatalogName(), table.getSchemaTableName().getSchemaName(), query);
      } catch (AccessDeniedException var6) {
         AccessDeniedException.denyDropTable(DBSAuthUtils.toString(catalogSchemaTableName), DBSAuthUtils.formatExceptionMessage(var6));
      }

   }

   public void checkCanRenameTable(Identity identity, CatalogSchemaTableName catalogSchemaTableName, CatalogSchemaTableName newTable) {
      CatalogSchemaTableName table = DBSAuthUtils.getHiveParityCatalog(catalogSchemaTableName);
      String query = DBSDdlOperations.ALTER_TABLE.getQuery(DBSAuthUtils.toString(table));

      try {
         this.policyHandler.checkAccess(identity, table.getCatalogName(), table.getSchemaTableName().getSchemaName(), query);
      } catch (AccessDeniedException var7) {
         AccessDeniedException.denyRenameTable(DBSAuthUtils.toString(catalogSchemaTableName), DBSAuthUtils.formatExceptionMessage(var7));
      }

   }

   public void checkCanShowTablesMetadata(Identity identity, CatalogSchemaName schema) {
   }

   public Set<SchemaTableName> filterTables(Identity identity, String catalogName, Set<SchemaTableName> tableNames) {
      return tableNames;
   }

   public void checkCanAddColumn(Identity identity, CatalogSchemaTableName catalogSchemaTableName) {
      CatalogSchemaTableName table = DBSAuthUtils.getHiveParityCatalog(catalogSchemaTableName);
      String query = DBSDdlOperations.ALTER_TABLE.getQuery(DBSAuthUtils.toString(table));

      try {
         this.policyHandler.checkAccess(identity, table.getCatalogName(), table.getSchemaTableName().getSchemaName(), query);
      } catch (AccessDeniedException var6) {
         AccessDeniedException.denyAddColumn(DBSAuthUtils.toString(catalogSchemaTableName), DBSAuthUtils.formatExceptionMessage(var6));
      }

   }

   public void checkCanDropColumn(Identity identity, CatalogSchemaTableName catalogSchemaTableName) {
      CatalogSchemaTableName table = DBSAuthUtils.getHiveParityCatalog(catalogSchemaTableName);
      String query = DBSDdlOperations.ALTER_TABLE.getQuery(DBSAuthUtils.toString(table));

      try {
         this.policyHandler.checkAccess(identity, table.getCatalogName(), table.getSchemaTableName().getSchemaName(), query);
      } catch (AccessDeniedException var6) {
         AccessDeniedException.denyDropColumn(DBSAuthUtils.toString(catalogSchemaTableName), DBSAuthUtils.formatExceptionMessage(var6));
      }

   }

   public void checkCanRenameColumn(Identity identity, CatalogSchemaTableName catalogSchemaTableName) {
      CatalogSchemaTableName table = DBSAuthUtils.getHiveParityCatalog(catalogSchemaTableName);
      String query = DBSDdlOperations.ALTER_TABLE.getQuery(DBSAuthUtils.toString(table));

      try {
         this.policyHandler.checkAccess(identity, table.getCatalogName(), table.getSchemaTableName().getSchemaName(), query);
      } catch (AccessDeniedException var6) {
         AccessDeniedException.denyRenameColumn(DBSAuthUtils.toString(catalogSchemaTableName), DBSAuthUtils.formatExceptionMessage(var6));
      }

   }

   public void checkCanInsertIntoTable(Identity identity, CatalogSchemaTableName catalogSchemaTableName) {
      CatalogSchemaTableName table = DBSAuthUtils.getHiveParityCatalog(catalogSchemaTableName);
      String query = DBSDdlOperations.INSERT_INTO_TABLE.getQuery(DBSAuthUtils.toString(table));

      try {
         this.policyHandler.checkAccess(identity, table.getCatalogName(), table.getSchemaTableName().getSchemaName(), query);
      } catch (AccessDeniedException var6) {
         AccessDeniedException.denyInsertTable(DBSAuthUtils.toString(catalogSchemaTableName), DBSAuthUtils.formatExceptionMessage(var6));
      }

   }

   public void checkCanDeleteFromTable(Identity identity, CatalogSchemaTableName catalogSchemaTableName) {
      CatalogSchemaTableName table = DBSAuthUtils.getHiveParityCatalog(catalogSchemaTableName);
      String query = DBSDdlOperations.DELETE_FROM_TABLE.getQuery(DBSAuthUtils.toString(table));

      try {
         this.policyHandler.checkAccess(identity, table.getCatalogName(), table.getSchemaTableName().getSchemaName(), query);
      } catch (AccessDeniedException var6) {
         AccessDeniedException.denyDeleteTable(DBSAuthUtils.toString(catalogSchemaTableName), DBSAuthUtils.formatExceptionMessage(var6));
      }

   }

   public void checkCanCreateView(Identity identity, CatalogSchemaTableName catalogSchemaTableName) {
      CatalogSchemaTableName view = DBSAuthUtils.getHiveParityCatalog(catalogSchemaTableName);
      String query = DBSDdlOperations.CREATE_VIEW.getQuery(DBSAuthUtils.toString(view));

      try {
         this.policyHandler.checkAccess(identity, view.getCatalogName(), view.getSchemaTableName().getSchemaName(), query);
      } catch (AccessDeniedException var6) {
         AccessDeniedException.denyCreateView(DBSAuthUtils.toString(catalogSchemaTableName), DBSAuthUtils.formatExceptionMessage(var6));
      }

   }

   public void checkCanDropView(Identity identity, CatalogSchemaTableName catalogSchemaTableName) {
      CatalogSchemaTableName view = DBSAuthUtils.getHiveParityCatalog(catalogSchemaTableName);
      String query = DBSDdlOperations.DROP_VIEW.getQuery(DBSAuthUtils.toString(view));

      try {
         this.policyHandler.checkAccess(identity, view.getCatalogName(), view.getSchemaTableName().getSchemaName(), query);
      } catch (AccessDeniedException var6) {
         AccessDeniedException.denyDropView(DBSAuthUtils.toString(catalogSchemaTableName), DBSAuthUtils.formatExceptionMessage(var6));
      }

   }

   public void checkCanCreateViewWithSelectFromColumns(Identity identity, CatalogSchemaTableName table, Set<String> columns) {
   }

   public void checkCanSetCatalogSessionProperty(Identity identity, String catalogName, String propertyName) {
   }

   public void checkCanGrantTablePrivilege(Identity identity, Privilege privilege, CatalogSchemaTableName table, PrestoPrincipal grantee, boolean withGrantOption) {
      AccessDeniedException.denyGrantTablePrivilege(privilege.toString(), DBSAuthUtils.toString(table), "Privileges can be granted only via DBS policy configuration");
   }

   public void checkCanRevokeTablePrivilege(Identity identity, Privilege privilege, CatalogSchemaTableName table, PrestoPrincipal revokee, boolean grantOptionFor) {
      AccessDeniedException.denyRevokeTablePrivilege(privilege.toString(), DBSAuthUtils.toString(table), "Privileges can be revoked only via DBS policy configuration");
   }

   public void checkCanSelectFromColumns(Identity identity, CatalogSchemaTableName table, Set<String> columns) {
   }
}
