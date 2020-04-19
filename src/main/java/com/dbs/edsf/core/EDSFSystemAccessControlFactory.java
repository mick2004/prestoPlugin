package com.dbs.edsf.core;

import io.prestosql.spi.security.SystemAccessControl;
import io.prestosql.spi.security.SystemAccessControlFactory;

import java.util.Map;

public class EDSFSystemAccessControlFactory implements SystemAccessControlFactory {
    @Override
    public String getName() {
        return "EDSFSystemAccessControl";
    }

    @Override
    public SystemAccessControl create(Map<String, String> config) {
        //TODO:Load Config
        return new EDSFSystemAccessControl();
    }
}
