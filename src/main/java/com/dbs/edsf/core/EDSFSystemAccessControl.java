package com.dbs.edsf.core;

import com.dbs.edsf.CacheModel.*;
import com.google.common.collect.ImmutableSet;
import io.prestosql.spi.connector.*;
import io.prestosql.spi.security.*;
import io.prestosql.spi.type.Type;

import javax.annotation.concurrent.Immutable;
import java.security.Principal;
import java.util.*;
import java.util.function.Supplier;

import static io.prestosql.spi.security.AccessDeniedException.denySelectColumns;
import static java.util.Objects.requireNonNull;


public class EDSFSystemAccessControl implements SystemAccessControl {
//public class EDSFSystemAccessControl extends EDSFAccessControlTest

    //EDSFAccessControlTest edsfAccessControlTest = new EDSFAccessControlTest();

    CacheMockData cacheMockData = new CacheMockData();
    UserProfileCache userProfileCache =  new UserProfileCache();
    UserTableOperationsCache userTableOperationsCache = new UserTableOperationsCache();
    TableProfileColumnsCache tableProfileColumnsCache = new TableProfileColumnsCache();
    String profile ;

    @Override
    public void checkCanKillQueryOwnedBy(SystemSecurityContext context, String queryOwner) {
        System.out.println("checkCanKillQueryOwnedBy  ==> ");
    }

    @Override
    public void checkCanDropSchema(SystemSecurityContext context, CatalogSchemaName schema) {
        System.out.println("checkCanDropSchema ==> ");
    }

    @Override
    public void checkCanRenameSchema(SystemSecurityContext context, CatalogSchemaName schema, String newSchemaName) {
        System.out.println("checkCanRenameSchema ==> ");
    }

    @Override
    public void checkCanSetSchemaAuthorization(SystemSecurityContext context, CatalogSchemaName schema, PrestoPrincipal principal) {
        System.out.println("checkCanSetSchemaAuthorization ==> ");
    }

    @Override
    public void checkCanShowCreateSchema(SystemSecurityContext context, CatalogSchemaName schemaName) {
        System.out.println("checkCanShowCreateSchema ==> ");
    }

    @Override
    public void checkCanDropTable(SystemSecurityContext context, CatalogSchemaTableName table) {
        System.out.println("checkCanDropTable ==> ");
/*        StackTraceElement[] stackTrace = Thread.currentThread().getStackTrace();
        System.out.println("getStackTrace()");
        for (int i = 1; i < stackTrace.length; i++)
            System.out.println("stacktrace ======>  " + stackTrace[i]);*/

        System.out.println(context.getIdentity().getUser() + "====> " + table.getSchemaTableName().getTableName());

    }

    @Override
    public void checkCanRenameTable(SystemSecurityContext context, CatalogSchemaTableName table, CatalogSchemaTableName newTable) {
        System.out.println("checkCanRenameTable ==> ");
    }

    @Override
    public void checkCanSetTableComment(SystemSecurityContext context, CatalogSchemaTableName table) {
        System.out.println("checkCanSetTableComment ==> ");
    }

    @Override
    public void checkCanShowColumns(SystemSecurityContext context, CatalogSchemaTableName table) {

        System.out.println("checkCanShowColumns ==> " + table.getSchemaTableName().getTableName());

    }

    @Override
    public void checkCanDropColumn(SystemSecurityContext context, CatalogSchemaTableName table) {
        System.out.println("checkCanDropColumn ==> ");
    }

    @Override
    public void checkCanRenameColumn(SystemSecurityContext context, CatalogSchemaTableName table) {
        System.out.println("checkCanRenameColumn ==> ");
    }

    @Override
    public void checkCanInsertIntoTable(SystemSecurityContext context, CatalogSchemaTableName table) {
        System.out.println("checkCanInsertIntoTable ==> ");
    }

    @Override
    public void checkCanDeleteFromTable(SystemSecurityContext context, CatalogSchemaTableName table) {
        System.out.println("checkCanDeleteFromTable ==> ");
    }

    @Override
    public void checkCanRenameView(SystemSecurityContext context, CatalogSchemaTableName view, CatalogSchemaTableName newView) {
        System.out.println("checkCanRenameView ==> ");
    }

    @Override
    public void checkCanSetCatalogSessionProperty(SystemSecurityContext context, String catalogName, String propertyName) {
        System.out.println("checkCanSetCatalogSessionProperty ==> ");
    }

    @Override
    public void checkCanGrantTablePrivilege(SystemSecurityContext context, Privilege privilege, CatalogSchemaTableName table, PrestoPrincipal grantee, boolean grantOption) {
        System.out.println("checkCanGrantTablePrivilege ==>");
    }

    @Override
    public void checkCanRevokeTablePrivilege(SystemSecurityContext context, Privilege privilege, CatalogSchemaTableName table, PrestoPrincipal revokee, boolean grantOption) {
        System.out.println("checkCanRevokeTablePrivilege ==> ");
    }

    @Override
    public void checkCanShowRoles(SystemSecurityContext context, String catalogName) {
        System.out.println("checkCanShowRoles ==> ");
    }


    @Override
    public void checkCanExecuteProcedure(SystemSecurityContext systemSecurityContext, CatalogSchemaRoutineName procedure) {
        System.out.println("checkCanExecuteProcedure ==> ");
    }


    @Override
    public List<ColumnMetadata> filterColumns(SystemSecurityContext context, CatalogSchemaTableName table, List<ColumnMetadata> columns) {
        System.out.println("filterColumns  ==> " + columns);
        /*columns.remove("city");*/
        return columns;
    }

    @Override
    public void checkCanAccessCatalog(SystemSecurityContext context, String catalogName) {
        //AccessDeniedException.denyCatalogAccess(catalogName);
        System.out.println("checkCanAccessCatalog ==> " + catalogName + "context.getIdentity().getUser() " + context.getIdentity().getUser());

    }

    @Override
    public Set<String> filterViewQueryOwnedBy(SystemSecurityContext context, Set<String> queryOwners) {
        System.out.println("filterViewQueryOwnedBy ==> " + queryOwners);
        System.out.println("context.getIdentity().getUser() " + context.getIdentity().getUser());
        return queryOwners;
    }

    @Override
    public void checkCanShowSchemas(SystemSecurityContext context, String catalogName) {
        //requireNonNull(catalogName, "catalogName is null");
        System.out.println("checkCanShowSchemas ==>");
        System.out.println("context.getIdentity().getUser() " + context.getIdentity().getUser());
    }

    @Override
    public Set<String> filterCatalogs(SystemSecurityContext context, Set<String> catalogs) {
        System.out.println("trying to filter catalogs ");
        System.out.println("filterCatalogs ==>" + context.getIdentity().getUser());
        System.out.println("context.getIdentity().getUser() " + context.getIdentity().getUser());
        return catalogs;
    }

    @Override
    public void checkCanGrantExecuteFunctionPrivilege(SystemSecurityContext context, String functionName, PrestoPrincipal grantee, boolean grantOption) {
        System.out.println(grantee.getName());
        System.out.println(context.getIdentity().getUser());

    }


    @Override
    public void checkCanExecuteQuery(SystemSecurityContext context) {
        System.out.println("checkCanExecuteQuery ==> " + context.getIdentity().getUser());
        System.out.println("context.getIdentity().getUser() " + context.getIdentity().getUser());
        EDSFSystemAccessControl edsfSystemAccessControl = new EDSFSystemAccessControl();
        if (edsfSystemAccessControl.userAuthStatus(context)){
            System.out.println("USER AUTHENTICATED");
        }else {
            AccessDeniedException.denyExecuteQuery();
        }

    }

    @Override
    public void checkCanShowTables(SystemSecurityContext context, CatalogSchemaName schema) {
        System.out.println("checkCanShowTables ==> " + schema.getSchemaName());
        System.out.println("user ==> " + context.getIdentity().getUser());
    }

    @Override
    public Set<SchemaTableName> filterTables(SystemSecurityContext context, String catalogName, Set<SchemaTableName> tableNames) {
        System.out.println("filterTables ==> " + catalogName);
        System.out.println("tableNames  ==> " + tableNames);
        System.out.println("context.getIdentity().getUser() " + context.getIdentity().getUser());
        return tableNames;
    }

    @Override
    public void checkCanImpersonateUser(SystemSecurityContext context, String userName) {
        System.out.println("checkCanImpersonateUser ==>    " + context + "  username  " + userName);
        System.out.println("context.getIdentity().getUser() " + context.getIdentity().getUser());


    }

    @Override
    public void checkCanExecuteFunction(SystemSecurityContext context, String functionName) {
        System.out.printf("checkCanExecuteFunction  ==> " + functionName + " by user ==> " + context.getIdentity().getUser());

    }

    @Override
    public void checkCanCreateViewWithSelectFromColumns(SystemSecurityContext context, CatalogSchemaTableName table, Set<String> columns) {
        System.out.println("checkCanCreateViewWithSelectFromColumns ==> " + context.getIdentity().getUser());
        //System.out.println("extra cred "+context.getIdentity().getExtraCredentials());

    }


    @Override
    public Set<String> filterSchemas(SystemSecurityContext context, String catalogName, Set<String> schemaNames) {
        System.out.println("filterSchemas ==>  ");
        System.out.println("context.getIdentity().getUser() " + context.getIdentity().getUser());
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
    public void checkCanAddColumn(SystemSecurityContext context, CatalogSchemaTableName table) {
        System.out.println("checkCanAddColumn ==> " + table.getSchemaTableName().getTableName());
        System.out.println("context.getIdentity().getUser() " + context.getIdentity().getUser());

    }

    @Override
    public void checkCanViewQueryOwnedBy(SystemSecurityContext context, String queryOwner) {
        System.out.println("checkCanViewQueryOwnedBy ==> " + queryOwner);
        System.out.println("context.getIdentity().getUser() " + context.getIdentity().getUser());

    }

    @Override
    public void checkCanCreateSchema(SystemSecurityContext context, CatalogSchemaName schema) {

        System.out.println("checkCanCreateSchema ==> " + schema.getSchemaName());
        System.out.println("context.getIdentity().getUser() " + context.getIdentity().getUser());
    }

    @Override
    public void checkCanShowCreateTable(SystemSecurityContext context, CatalogSchemaTableName table) {

        System.out.println("checkCanShowCreateTable ==> " + table.getSchemaTableName().getTableName());
        System.out.println("context.getIdentity().getUser() " + context.getIdentity().getUser());

    }

    @Override
    public void checkCanCreateTable(SystemSecurityContext context, CatalogSchemaTableName table) {

        System.out.println("checkCanCreateTable  ==> " + table.getSchemaTableName().getTableName());
        System.out.println("context.getIdentity().getUser() " + context.getIdentity().getUser());
    }

    @Override
    public void checkCanCreateView(SystemSecurityContext context, CatalogSchemaTableName view) {

        System.out.println("checkCanCreateView ==> " + view.getSchemaTableName().getTableName());
        System.out.println("context.getIdentity().getUser() " + context.getIdentity().getUser());
    }

    @Override
    public void checkCanDropView(SystemSecurityContext context, CatalogSchemaTableName view) {

        System.out.println("checkCanDropView  ==> " + view.getSchemaTableName().getTableName());
        System.out.println("context.getIdentity().getUser() " + context.getIdentity().getUser());
    }

    @Override
    public void checkCanSelectFromColumns(SystemSecurityContext context, CatalogSchemaTableName table, Set<String> columns) {

        userTableOperationsCache = cacheMockData.UserTableOperationsCacheLoad();
        System.out.println("userTableOperationsCache.getHm();" +userTableOperationsCache.getHm());
        Map<String, Map<String, UserOperationRLFMapping>> uTrlfMap = userTableOperationsCache.getHm();

        if (uTrlfMap.get(context.getIdentity().getUser()).containsKey(table.getSchemaTableName().getTableName())) {

            System.out.println("profile in checkCanSelectFromColumns " + profile);
            tableProfileColumnsCache = cacheMockData.tableProfileColumnsCacheLoad();
            System.out.println(tableProfileColumnsCache.getUserTableProfilemap().get(table.getSchemaTableName().getTableName()).ProfileColumnsMap.get(profile));

            Set<String> profileCols = new HashSet<>();
            for (String key : tableProfileColumnsCache.getUserTableProfilemap().get(table.getSchemaTableName().getTableName()).ProfileColumnsMap.get(profile).keySet()) {
                System.out.println("key is " + key);
                profileCols.add(key);
            }

            Set<String> denyCols = new HashSet<>();
            columns.removeAll(profileCols);
            for (String col : columns) {
                System.out.println("cols in table are " + col);
                denyCols.add(col);
            }

            if (denyCols.size() > 0) {
                AccessDeniedException.denySelectColumns(table.getSchemaTableName().getTableName(), denyCols);
            }
        }
    }

    @Override
    public Optional<ViewExpression> getRowFilter(SystemSecurityContext context, CatalogSchemaTableName tableName) {
        System.out.println("getRowFilter ==> " + "for tablename " + tableName.getSchemaTableName().getTableName() + " by user " + context.getIdentity().getUser());

        userTableOperationsCache = cacheMockData.UserTableOperationsCacheLoad();
        System.out.println("userTableOperationsCache.getHm();" +userTableOperationsCache.getHm());
        Map<String, Map<String, UserOperationRLFMapping>> uTrlfMap = userTableOperationsCache.getHm();

        if (uTrlfMap.get(context.getIdentity().getUser()).containsKey(tableName.getSchemaTableName().getTableName())) {
            String rowFilter1 = uTrlfMap.get(context.getIdentity().getUser()).get(tableName.getSchemaTableName().getTableName()).rowLevelFilter.get(0);

            //String rowFilter2 = uTrlfMap.get(context.getIdentity().getUser()).get(tableName.getSchemaTableName().getTableName()).rowLevelFilter.get(1);
            //String expression = "lower (col3) = lower( "+ "\'" +rowFilter1 +"\')" +" and lower (col4) = lower( "+ "\'" +rowFilter2 +"\')" ;

            String expression = "lower (col3) = lower( "+ "\'" +rowFilter1 +"\')" ;
            Optional<ViewExpression> viewExpression = Optional.of(
                    new ViewExpression(context.getIdentity().getUser(), Optional.of(tableName.getCatalogName()), Optional.of(tableName.getSchemaTableName().getTableName()), expression));
            System.out.println("view expression == > " + viewExpression.get().getExpression());
            System.out.println(Optional.of(tableName.getCatalogName()));
            System.out.println(Optional.of(tableName.getSchemaTableName().getTableName()));
            return viewExpression ;

        }
        return Optional.empty();

    }

    @Override
    public Optional<ViewExpression> getColumnMask(SystemSecurityContext context, CatalogSchemaTableName tableName, String columnName, Type type) {

        // ViewExpression -> Identity, catalog, schema, expression
        System.out.println("getColumnMask ==> ");
        System.out.println("TableName :" + tableName.getSchemaTableName().getTableName() + ",columnName " + columnName + ",Type :" + type);
        System.out.println("username  " + context.getIdentity().getUser());

        // Allow access for 'show schemas' and 'show tables'
        if (tableName.getSchemaTableName().getTableName().equals("schemata")||tableName.getSchemaTableName().getTableName().equals("tables")){
            return Optional.empty();
        }

        userTableOperationsCache = cacheMockData.UserTableOperationsCacheLoad();
        System.out.println("userTableOperationsCache.getHm();" +userTableOperationsCache.getHm());
        Map<String, Map<String, UserOperationRLFMapping>> uTrlfMap = userTableOperationsCache.getHm();

        String username = context.getIdentity().getUser() ;
        System.out.println("username " + username);
        System.out.println(uTrlfMap.get(username).containsKey(tableName.getSchemaTableName().getTableName()));

        tableProfileColumnsCache = cacheMockData.tableProfileColumnsCacheLoad();


        // check if the given user has access to query the table
        if (uTrlfMap.get(context.getIdentity().getUser()).containsKey(tableName.getSchemaTableName().getTableName())){
            if (tableProfileColumnsCache.getUserTableProfilemap().get(tableName.getSchemaTableName().getTableName().toLowerCase()).ProfileColumnsMap.containsKey("all")){

                System.out.println("contains all for "+ columnName);
                profile = "all" ;
                System.out.println("profile is "+ profile);
                String expression = tableProfileColumnsCache.getUserTableProfilemap().get(tableName.getSchemaTableName().getTableName().toLowerCase()).ProfileColumnsMap.get("all").get(columnName);
                System.out.println("expression "+ expression);
                if (expression == null){
                    return Optional.empty();
                }
                Optional<ViewExpression> viewExpression = Optional.of(
                        new ViewExpression(context.getIdentity().getUser(), Optional.of(tableName.getCatalogName()), Optional.of(tableName.getSchemaTableName().getTableName()), expression));
                System.out.println("view expression == > " + viewExpression.get().getExpression());
                System.out.println(Optional.of(tableName.getCatalogName()));
                System.out.println(Optional.of(tableName.getSchemaTableName().getTableName()));

                return viewExpression;

            }else if (tableProfileColumnsCache.getUserTableProfilemap().get(tableName.getSchemaTableName().getTableName().toLowerCase()).ProfileColumnsMap.containsKey("ns_pni")){
                System.out.println("ns_pni");
                profile = "ns_pni" ;
                System.out.println("profile is "+ profile);
                String expression = tableProfileColumnsCache.getUserTableProfilemap().get(tableName.getSchemaTableName().getTableName().toLowerCase()).ProfileColumnsMap.get("ns_pni").get(columnName);
                if (expression == null){
                    return Optional.empty();
                }
                Optional<ViewExpression> viewExpression = Optional.of(
                        new ViewExpression(context.getIdentity().getUser(), Optional.of(tableName.getCatalogName()), Optional.of(tableName.getSchemaTableName().getTableName()), expression));
                System.out.println("view expression == > " + viewExpression.get().getExpression());
                System.out.println(Optional.of(tableName.getCatalogName()));
                System.out.println(Optional.of(tableName.getSchemaTableName().getTableName()));

                return viewExpression;


            }else if (tableProfileColumnsCache.getUserTableProfilemap().get(tableName.getSchemaTableName().getTableName().toLowerCase()).ProfileColumnsMap.containsKey("ns")){
                System.out.println("ns");
                profile = "ns" ;
                System.out.println("profile is "+ profile);
                String expression = tableProfileColumnsCache.getUserTableProfilemap().get(tableName.getSchemaTableName().getTableName().toLowerCase()).ProfileColumnsMap.get("ns").get(columnName);
                if (expression == null){
                    return Optional.empty();
                }
                Optional<ViewExpression> viewExpression = Optional.of(
                        new ViewExpression(context.getIdentity().getUser(), Optional.of(tableName.getCatalogName()), Optional.of(tableName.getSchemaTableName().getTableName()), expression));
                System.out.println("view expression == > " + viewExpression.get().getExpression());
                System.out.println(Optional.of(tableName.getCatalogName()));
                System.out.println(Optional.of(tableName.getSchemaTableName().getTableName()));

                return viewExpression;
            }else{
                AccessDeniedException.denyExecuteQuery();
            }
        }else {
            AccessDeniedException.denyExecuteQuery();
        }

        return Optional.empty();

    }
    public boolean userAuthStatus(SystemSecurityContext context){
        userProfileCache = cacheMockData.UserProfileCacheLoad();
        Map<String, Map<String,String>> upMap= userProfileCache.getUserProfileMap();
        Boolean profile =upMap.containsKey(context.getIdentity().getUser());
        return profile;
    }


}
