package com.dbs.edsf.presto.authorization.ast;

import com.google.common.collect.Lists;
import io.prestosql.metadata.MetadataUtil;
import io.prestosql.metadata.QualifiedObjectName;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.StandardErrorCode;
import io.prestosql.spi.StatementRewriteContext;
import io.prestosql.spi.connector.CatalogSchemaName;
import io.prestosql.spi.connector.CatalogSchemaTableName;
import io.prestosql.spi.security.Privilege;
import io.prestosql.spi.security.SystemAccessControl;
//import io.prestosql.sql.analyzer.SemanticErrorCode;
//import io.prestosql.sql.analyzer.SemanticException;
import io.prestosql.sql.analyzer.SemanticErrorCode;
import io.prestosql.sql.analyzer.SemanticException;
import io.prestosql.sql.tree.AddColumn;
import io.prestosql.sql.tree.AstVisitor;
import io.prestosql.sql.tree.CreateSchema;
import io.prestosql.sql.tree.CreateTable;
import io.prestosql.sql.tree.CreateTableAsSelect;
import io.prestosql.sql.tree.CreateView;
import io.prestosql.sql.tree.Delete;
import io.prestosql.sql.tree.DropColumn;
import io.prestosql.sql.tree.DropSchema;
import io.prestosql.sql.tree.DropTable;
import io.prestosql.sql.tree.DropView;
import io.prestosql.sql.tree.Grant;
import io.prestosql.sql.tree.Node;
import io.prestosql.sql.tree.QualifiedName;
import io.prestosql.sql.tree.Query;
import io.prestosql.sql.tree.RenameColumn;
import io.prestosql.sql.tree.RenameSchema;
import io.prestosql.sql.tree.RenameTable;
import io.prestosql.sql.tree.Revoke;
import io.prestosql.sql.tree.Table;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

public abstract class DBSAstVisitor extends AstVisitor<Node, DBSAstVisitorScope> {
   private final StatementRewriteContext session;
   private final SystemAccessControl accessControl;

   protected DBSAstVisitor(StatementRewriteContext session, SystemAccessControl accessControl) {
      this.session = session;
      this.accessControl = accessControl;
   }

   protected Node visitCreateSchema(CreateSchema createSchema, DBSAstVisitorScope context) {
      CatalogSchemaName catalogSchemaName = createCatalogSchemaName(this.session, createSchema, Optional.of(createSchema.getSchemaName()));
      //this.accessControl.checkCanCreateSchema(this.session.getIdentity(), catalogSchemaName);
      return this.visitNode(createSchema, (DBSAstVisitorScope)context);
   }

   protected Node visitDropSchema(DropSchema dropSchema, DBSAstVisitorScope context) {
      QualifiedObjectName schemaName = createQualifiedObjectName(this.session, dropSchema, dropSchema.getSchemaName());
      //this.accessControl.checkCanDropTable(this.session.getIdentity(), schemaName.asCatalogSchemaTableName());
      return this.visitNode(dropSchema, (DBSAstVisitorScope)context);
   }

   protected Node visitRenameSchema(RenameSchema renameSchema, DBSAstVisitorScope context) {
      CatalogSchemaName catalogSchemaName = createCatalogSchemaName(this.session, renameSchema, Optional.of(renameSchema.getSource()));
      //this.accessControl.checkCanRenameSchema(this.session.getIdentity(), catalogSchemaName, renameSchema.getTarget().getValue());
      return this.visitNode(renameSchema, (DBSAstVisitorScope)context);
   }

   protected Node visitCreateTable(CreateTable createTable, DBSAstVisitorScope context) {
      QualifiedObjectName targetTable = createQualifiedObjectName(this.session, createTable, createTable.getName());
      //this.accessControl.checkCanCreateTable(this.session.getIdentity(), targetTable.asCatalogSchemaTableName());
      return this.visitNode(createTable, (DBSAstVisitorScope)context);
   }

   protected Node visitCreateTableAsSelect(CreateTableAsSelect createTableAsSelect, DBSAstVisitorScope context) {
      QualifiedObjectName targetTable = createQualifiedObjectName(this.session, createTableAsSelect, createTableAsSelect.getName());
      //this.accessControl.checkCanCreateTable(this.session.getIdentity(), targetTable.asCatalogSchemaTableName());
      Query query = (Query)this.visitQuery(createTableAsSelect.getQuery(), context);
      return (Node)createTableAsSelect.getLocation().map((nodeLocation) -> {
         return new CreateTableAsSelect(nodeLocation, createTableAsSelect.getName(), query, createTableAsSelect.isNotExists(), createTableAsSelect.getProperties(), createTableAsSelect.isWithData(), createTableAsSelect.getColumnAliases(), createTableAsSelect.getComment());
      }).orElse(new CreateTableAsSelect(createTableAsSelect.getName(), query, createTableAsSelect.isNotExists(), createTableAsSelect.getProperties(), createTableAsSelect.isWithData(), createTableAsSelect.getColumnAliases(), createTableAsSelect.getComment()));
   }

   protected Node visitDropTable(DropTable dropTable, DBSAstVisitorScope context) {
      QualifiedObjectName targetTable = createQualifiedObjectName(this.session, dropTable, dropTable.getTableName());
      //this.accessControl.checkCanDropTable(this.session.getIdentity(), targetTable.asCatalogSchemaTableName());
      return this.visitNode(dropTable, (DBSAstVisitorScope)context);
   }

   protected Node visitRenameTable(RenameTable renameTable, DBSAstVisitorScope context) {
      QualifiedObjectName targetTable = createQualifiedObjectName(this.session, renameTable, renameTable.getTarget());
      QualifiedObjectName sourceTable = createQualifiedObjectName(this.session, renameTable, renameTable.getSource());
      //this.accessControl.checkCanRenameTable(this.session.getIdentity(), sourceTable.asCatalogSchemaTableName(), targetTable.asCatalogSchemaTableName());
      return this.visitNode(renameTable, (DBSAstVisitorScope)context);
   }

   protected Node visitAddColumn(AddColumn addColumnNode, DBSAstVisitorScope context) {
      QualifiedObjectName tableName = createQualifiedObjectName(this.session, addColumnNode, addColumnNode.getName());
      //this.accessControl.checkCanAddColumn(this.session.getIdentity(), tableName.asCatalogSchemaTableName());
      return this.visitNode(addColumnNode, (DBSAstVisitorScope)context);
   }

   protected Node visitDropColumn(DropColumn dropColumn, DBSAstVisitorScope context) {
      QualifiedObjectName tableName = createQualifiedObjectName(this.session, dropColumn, dropColumn.getTable());
      //this.accessControl.checkCanDropColumn(this.session.getIdentity(), tableName.asCatalogSchemaTableName());
      return this.visitNode(dropColumn, (DBSAstVisitorScope)context);
   }

   protected Node visitRenameColumn(RenameColumn renameColumn, DBSAstVisitorScope context) {
      QualifiedObjectName tableName = createQualifiedObjectName(this.session, renameColumn, renameColumn.getTable());
      //this.accessControl.checkCanRenameColumn(this.session.getIdentity(), tableName.asCatalogSchemaTableName());
      return this.visitNode(renameColumn, (DBSAstVisitorScope)context);
   }

   protected Node visitDelete(Delete delete, DBSAstVisitorScope context) {
      Table table = delete.getTable();
      QualifiedObjectName targetTable = createQualifiedObjectName(this.session, table, table.getName());
      //this.accessControl.checkCanDeleteFromTable(this.session.getIdentity(), targetTable.asCatalogSchemaTableName());
      return this.visitNode(delete, (DBSAstVisitorScope)context);
   }

   protected Node visitCreateView(CreateView createView, DBSAstVisitorScope context) {
      QualifiedObjectName viewName = createQualifiedObjectName(this.session, createView, createView.getName());
      //this.accessControl.checkCanCreateView(this.session.getIdentity(), viewName.asCatalogSchemaTableName());
      return this.visitNode(createView, (DBSAstVisitorScope)context);
   }

   protected Node visitDropView(DropView dropView, DBSAstVisitorScope context) {
      QualifiedObjectName viewName = createQualifiedObjectName(this.session, dropView, dropView.getName());
      //this.accessControl.checkCanDropView(this.session.getIdentity(), viewName.asCatalogSchemaTableName());
      return this.visitNode(dropView, (DBSAstVisitorScope)context);
   }

   protected Node visitGrant(Grant grant, DBSAstVisitorScope context) {
      QualifiedObjectName tableName = createQualifiedObjectName(this.session, grant, grant.getTableName());
      CatalogSchemaTableName catTableName = tableName.asCatalogSchemaTableName();
      grant.getPrivileges().ifPresent((privileges) -> {
         privileges.forEach((access) -> {
            //this.accessControl.checkCanGrantTablePrivilege(this.session.getIdentity(), Privilege.valueOf(access), catTableName, MetadataUtil.createPrincipal(grant.getGrantee()), grant.isWithGrantOption());
         });
      });
      return this.visitNode(grant, (DBSAstVisitorScope)context);
   }

   protected Node visitRevoke(Revoke revoke, DBSAstVisitorScope context) {
      QualifiedObjectName tableName = createQualifiedObjectName(this.session, revoke, revoke.getTableName());
      CatalogSchemaTableName catTableName = tableName.asCatalogSchemaTableName();
      revoke.getPrivileges().ifPresent((privileges) -> {
         privileges.forEach((access) -> {
            //this.accessControl.checkCanRevokeTablePrivilege(this.session.getIdentity(), Privilege.valueOf(access), catTableName, MetadataUtil.createPrincipal(revoke.getGrantee()), revoke.isGrantOptionFor());
         });
      });
      return this.visitNode(revoke, (DBSAstVisitorScope)context);
   }

   protected Node visitNode(Node node, DBSAstVisitorScope context) {
      return node;
   }

   public static CatalogSchemaName createCatalogSchemaName(StatementRewriteContext session, Node node, Optional<QualifiedName> schema) {
      String catalogName = (String)session.getCatalog().orElse("");
      String schemaName = (String)session.getSchema().orElse("");
      if (schema.isPresent()) {
         List<String> parts = ((QualifiedName)schema.get()).getParts();
         if (parts.size() > 2) {
            //throw new SemanticException(SemanticErrorCode.INVALID_SCHEMA_NAME, node, "Too many parts in schema name: %s", new Object[]{schema.get()});
         }

         if (parts.size() == 2) {
            catalogName = (String)parts.get(0);
         }

         schemaName = ((QualifiedName)schema.get()).getSuffix();
      }

      if (catalogName == null) {
         throw new SemanticException(SemanticErrorCode.CATALOG_NOT_SPECIFIED, node, "Catalog must be specified when session catalog is not set", new Object[0]);
      } else if (schemaName == null) {
         throw new SemanticException(SemanticErrorCode.SCHEMA_NOT_SPECIFIED, node, "Schema must be specified when session schema is not set", new Object[0]);
      } else {
         return new CatalogSchemaName(catalogName, schemaName);
      }
   }

   public static QualifiedObjectName createQualifiedObjectName(StatementRewriteContext session, Node node, QualifiedName name) {
      Objects.requireNonNull(session, "session is null");
      Objects.requireNonNull(name, "name is null");
      if (name.getParts().size() > 3) {
         throw new PrestoException(StandardErrorCode.SYNTAX_ERROR, String.format("Too many dots in table name: %s", name));
      } else {
         List<String> parts = Lists.reverse(name.getParts());
         String objectName = (String)parts.get(0);
         String schemaName = parts.size() > 1 ? (String)parts.get(1) : (String)session.getSchema().orElseThrow(() -> {
            return new SemanticException(SemanticErrorCode.SCHEMA_NOT_SPECIFIED, node, "Schema must be specified when session schema is not set", new Object[0]);
         });
         String catalogName = parts.size() > 2 ? (String)parts.get(2) : (String)session.getCatalog().orElseThrow(() -> {
            return new SemanticException(SemanticErrorCode.CATALOG_NOT_SPECIFIED, node, "Catalog must be specified when session catalog is not set", new Object[0]);
         });
         return new QualifiedObjectName(catalogName, schemaName, objectName);
      }
   }
}
