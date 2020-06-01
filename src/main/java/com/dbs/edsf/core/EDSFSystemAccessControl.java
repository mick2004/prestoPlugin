package com.dbs.edsf.core;

import io.prestosql.spi.connector.CatalogSchemaName;
import io.prestosql.spi.connector.CatalogSchemaTableName;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.spi.security.*;
import io.prestosql.spi.security.Identity;
import io.prestosql.spi.type.Type;

import java.security.Principal;
import java.util.Optional;
import java.util.Set;

import static java.util.Objects.requireNonNull;

public class EDSFSystemAccessControl implements SystemAccessControl {


    @Override
    public void checkCanAccessCatalog(SystemSecurityContext context, String catalogName) {
        //AccessDeniedException.denyCatalogAccess(catalogName);
        System.out.println("the catalog name getting accessed is "+ catalogName);
        System.out.println(context.getIdentity().getUser());

    }

    @Override
    public Set<String> filterViewQueryOwnedBy(SystemSecurityContext context, Set<String> queryOwners) {
        System.out.println("who owns the query "+queryOwners);
        System.out.println(context.getIdentity().getUser());
        return queryOwners;
    }

    @Override
    public void checkCanShowSchemas(SystemSecurityContext context, String catalogName) {
        requireNonNull(catalogName, "catalogName is null");
        System.out.println(context.getIdentity().getUser());
    }

    @Override
    public Set<String> filterCatalogs(SystemSecurityContext context, Set<String> catalogs) {
        System.out.println("trying to filter catalogs ");
        System.out.println(context.getIdentity().getUser());
        return catalogs;
    }

    @Override
    public void checkCanGrantExecuteFunctionPrivilege(SystemSecurityContext context, String functionName, PrestoPrincipal grantee, boolean grantOption) {
        System.out.println(grantee.getName());
        System.out.println(context.getIdentity().getUser());

    }



    @Override
    public void checkCanExecuteQuery(SystemSecurityContext context) {
        System.out.println("1st entry point  =====");
        System.out.println("trying to see if can execute query by " + context.getIdentity().getUser());


    }

    @Override
    public void checkCanShowTables(SystemSecurityContext context, CatalogSchemaName schema) {
        System.out.println("allowing to see the tables in "+ schema);
        System.out.println(context.getIdentity().getUser());
    }

    @Override
    public Set<SchemaTableName> filterTables(SystemSecurityContext context, String catalogName, Set<SchemaTableName> tableNames) {
        System.out.println("tablenames in " +catalogName);
        System.out.println(context.getIdentity().getUser());
        return tableNames;
    }

    @Override
    public void checkCanImpersonateUser(SystemSecurityContext context, String userName) {
        System.out.println("check impersonate user    "+context +  "  username  "+userName);
        System.out.println(context.getIdentity().getUser());


    }

    @Override
    public void checkCanExecuteFunction(SystemSecurityContext context, String functionName) {
        System.out.printf("trying to see if can create function");
        System.out.println(context.getIdentity().getUser());

    }

    @Override
    public void checkCanCreateViewWithSelectFromColumns(SystemSecurityContext context, CatalogSchemaTableName table, Set<String> columns) {
        System.out.println("select from view by user "+context.getIdentity().getUser());
        System.out.println("extra cred "+context.getIdentity().getExtraCredentials());

    }

    @Override
    public void checkCanSelectFromColumns(SystemSecurityContext context, CatalogSchemaTableName table, Set<String> columns) {
        //AccessDeniedException.denySelectColumns(table.toString(), columns);

        System.out.println("trying to see if can select from columns"+context);
        System.out.println(context.getIdentity().getUser());
    }




    @Override
    public Set<String> filterSchemas(SystemSecurityContext context, String catalogName, Set<String> schemaNames) {
        System.out.println("tyring to see filter schema method");
        System.out.println(context.getIdentity().getUser());
        return schemaNames;

    }

    @Override
    public void checkCanSetSystemSessionProperty(SystemSecurityContext context, String propertyName) {
        //int i=5;

    }

    @Override
    public void checkCanSetUser(Optional<Principal> principal, String userName) {
        //int k=5;

    }


    @Override
    public Optional<ViewExpression> getRowFilter(SystemSecurityContext context, CatalogSchemaTableName tableName) {
        System.out.println("inside getrowfilter method ");
        System.out.println("tableName is :"+tableName);
        System.out.println(context.getIdentity().getUser());
        return Optional.empty();
    }

    @Override
    public Optional<ViewExpression> getColumnMask(SystemSecurityContext context, CatalogSchemaTableName tableName, String columnName, Type type) {
        System.out.println("inside getcolumn mask method");
        System.out.println("TableName :"+tableName +",columnName "+columnName+",Type :"+type);
        System.out.println("username  "+ context.getIdentity().getUser());
        System.out.println(tableName.getSchemaTableName());
        //new ViewExpression(context.getIdentity().getUser() ,Optional.empty(), Optional.empty(), "maskdata("+columnName+")");

        // need to make a call from here for querying policies to see if access to the views are there or not.

        return Optional.empty();
        //final Optional<ViewExpression> viewExpression = Optional.of(
        //  new ViewExpression("abhisheksingh" ,Optional.empty(), Optional.empty(), "mask("+columnName+")"));
        //return viewExpression;


    }
}
