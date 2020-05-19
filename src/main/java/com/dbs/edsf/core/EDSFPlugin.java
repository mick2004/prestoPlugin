package com.dbs.edsf.core;

import io.prestosql.spi.Plugin;
import io.prestosql.spi.security.SystemAccessControlFactory;

import java.util.Arrays;

public class EDSFPlugin implements Plugin {

    @Override
    public Iterable<SystemAccessControlFactory> getSystemAccessControlFactories() {
        return Arrays.asList(new EDSFSystemAccessControlFactory());
    }


}
