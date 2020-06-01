package com.dbs.edsf.core;

import io.prestosql.spi.security.SystemAccessControl;
import io.prestosql.spi.security.SystemAccessControlFactory;

import java.util.Map;

public class EDSFSystemAccessControlFactory implements SystemAccessControlFactory {
    @Override
    // Below step is to activate the plugin
    public String getName() {
        System.out.println("String getName ==> to activate Plugin ==> EDSFSystemAccessControl ");
        return "EDSFSystemAccessControl";
    }

    @Override
    // implemented to override SystemAccessControl implementation
    public SystemAccessControl create(Map<String, String> config) {
        //TODO:Load Config

        // load jar from here and test for dummy data
        System.out.println("overriding SystemAccessControl implementation ====> SystemAccessControl create ==> ");
        return new EDSFSystemAccessControl();

    }
}
