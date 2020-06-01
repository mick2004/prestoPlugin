/*
package com.dbs.edsf.core;

import io.prestosql.spi.connector.*;
import io.prestosql.spi.security.*;
import io.prestosql.spi.type.Type;

import java.security.Principal;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;

import static java.util.Objects.requireNonNull;

public class  EDSFAccessControlTest implements SystemAccessControl {



    @Override
    public void checkCanImpersonateUser(SystemSecurityContext context, String userName) {

    }

    @Override
    public void checkCanSetUser(Optional<Principal> principal, String userName) {

    }

    @Override
    public void checkCanExecuteQuery(SystemSecurityContext context) {

    }

    @Override
    public void checkCanViewQueryOwnedBy(SystemSecurityContext context, String queryOwner) {

    }

    @Override
    public Set<String> filterViewQueryOwnedBy(SystemSecurityContext context, Set<String> queryOwners) {
        return null;
    }

    @Override
    public void checkCanKillQueryOwnedBy(SystemSecurityContext context, String queryOwner) {

    }

    @Override
    public void checkCanSetSystemSessionProperty(SystemSecurityContext context, String propertyName) {

    }

    @Override
    public void checkCanAccessCatalog(SystemSecurityContext context, String catalogName) {

    }

    @Override
    public Set<String> filterCatalogs(SystemSecurityContext context, Set<String> catalogs) {
        return null;
    }

    @Override
    public void checkCanCreateSchema(SystemSecurityContext context, CatalogSchemaName schema) {

    }

    @Override
    public void checkCanDropSchema(SystemSecurityContext context, CatalogSchemaName schema) {

    }

    @Override
    public void checkCanRenameSchema(SystemSecurityContext context, CatalogSchemaName schema, String newSchemaName) {

    }

    @Override
    public void checkCanSetSchemaAuthorization(SystemSecurityContext context, CatalogSchemaName schema, PrestoPrincipal principal) {

    }

    @Override
    public void checkCanShowSchemas(SystemSecurityContext context, String catalogName) {

    }

    @Override
    public Set<String> filterSchemas(SystemSecurityContext context, String catalogName, Set<String> schemaNames) {
        return null;
    }

    @Override
    public void checkCanShowCreateSchema(SystemSecurityContext context, CatalogSchemaName schemaName) {

    }

    @Override
    public void checkCanShowCreateTable(SystemSecurityContext context, CatalogSchemaTableName table) {

    }

    @Override
    public void checkCanCreateTable(SystemSecurityContext context, CatalogSchemaTableName table) {

    }

    @Override
    public void checkCanDropTable(SystemSecurityContext context, CatalogSchemaTableName table) {

    }

    @Override
    public void checkCanRenameTable(SystemSecurityContext context, CatalogSchemaTableName table, CatalogSchemaTableName newTable) {

    }

    @Override
    public void checkCanSetTableComment(SystemSecurityContext context, CatalogSchemaTableName table) {

    }

    @Override
    public void checkCanShowTables(SystemSecurityContext context, CatalogSchemaName schema) {

    }

    @Override
    public Set<SchemaTableName> filterTables(SystemSecurityContext context, String catalogName, Set<SchemaTableName> tableNames) {
        return null;
    }

    @Override
    public void checkCanShowColumns(SystemSecurityContext context, CatalogSchemaTableName table) {

    }

    @Override
    public List<ColumnMetadata> filterColumns(SystemSecurityContext context, CatalogSchemaTableName table, List<ColumnMetadata> columns) {
        return null;
    }

    @Override
    public void checkCanAddColumn(SystemSecurityContext context, CatalogSchemaTableName table) {

    }

    @Override
    public void checkCanDropColumn(SystemSecurityContext context, CatalogSchemaTableName table) {

    }

    @Override
    public void checkCanRenameColumn(SystemSecurityContext context, CatalogSchemaTableName table) {

    }

    @Override
    public void checkCanSelectFromColumns(SystemSecurityContext context, CatalogSchemaTableName table, Set<String> columns) {
        for (String column:columns)
            System.out.println("columns *************** >>  "+column);

    }

    @Override
    public void checkCanInsertIntoTable(SystemSecurityContext context, CatalogSchemaTableName table) {

    }

    @Override
    public void checkCanDeleteFromTable(SystemSecurityContext context, CatalogSchemaTableName table) {

    }

    @Override
    public void checkCanCreateView(SystemSecurityContext context, CatalogSchemaTableName view) {

    }

    @Override
    public void checkCanRenameView(SystemSecurityContext context, CatalogSchemaTableName view, CatalogSchemaTableName newView) {

    }

    @Override
    public void checkCanDropView(SystemSecurityContext context, CatalogSchemaTableName view) {

    }

    @Override
    public void checkCanCreateViewWithSelectFromColumns(SystemSecurityContext context, CatalogSchemaTableName table, Set<String> columns) {

    }

    @Override
    public void checkCanGrantExecuteFunctionPrivilege(SystemSecurityContext context, String functionName, PrestoPrincipal grantee, boolean grantOption) {

    }

    @Override
    public void checkCanSetCatalogSessionProperty(SystemSecurityContext context, String catalogName, String propertyName) {

    }

    @Override
    public void checkCanGrantTablePrivilege(SystemSecurityContext context, Privilege privilege, CatalogSchemaTableName table, PrestoPrincipal grantee, boolean grantOption) {

    }

    @Override
    public void checkCanRevokeTablePrivilege(SystemSecurityContext context, Privilege privilege, CatalogSchemaTableName table, PrestoPrincipal revokee, boolean grantOption) {

    }

    @Override
    public void checkCanShowRoles(SystemSecurityContext context, String catalogName) {

    }

    @Override
    public void checkCanExecuteProcedure(SystemSecurityContext systemSecurityContext, CatalogSchemaRoutineName procedure) {

    }

    @Override
    public void checkCanExecuteFunction(SystemSecurityContext systemSecurityContext, String functionName) {

    }

    @Override
    public Optional<ViewExpression> getRowFilter(SystemSecurityContext context, CatalogSchemaTableName tableName) {
        return Optional.empty();
    }

    @Override
    public Optional<ViewExpression> getColumnMask(SystemSecurityContext context, CatalogSchemaTableName tableName, String columnName, Type type) {
        return Optional.empty();
    }
}
*/
