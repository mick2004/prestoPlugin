package com.dbs.edsf.core;

import io.prestosql.spi.connector.CatalogSchemaTableName;
import io.prestosql.spi.security.SystemAccessControl;
import io.prestosql.spi.security.SystemSecurityContext;
import io.prestosql.spi.security.ViewExpression;
import io.prestosql.spi.type.Type;

import java.security.Principal;
import java.util.Optional;

public class EDSFSystemAccessControl implements SystemAccessControl {

    @Override
    public void checkCanSetSystemSessionProperty(SystemSecurityContext context, String propertyName) {
        int i=5;

    }

    @Override
    public void checkCanSetUser(Optional<Principal> principal, String userName) {
        int k=5;

    }

    @Override
    public void checkCanExecuteQuery(SystemSecurityContext context) {

    }

    @Override
    public Optional<ViewExpression> getRowFilter(SystemSecurityContext context, CatalogSchemaTableName tableName) {
        return Optional.empty();
    }

    @Override
    public Optional<ViewExpression> getColumnMask(SystemSecurityContext context, CatalogSchemaTableName tableName, String columnName, Type type) {
        //return Optional.empty();
        final Optional<ViewExpression> viewExpression = Optional.of(
                new ViewExpression("abhisheksingh" ,Optional.empty(), Optional.empty(), "mask("+columnName+")"));
        return viewExpression;


    }
}
