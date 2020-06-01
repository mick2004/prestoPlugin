package com.dbs.edsf.core;

import io.prestosql.spi.Plugin;
import io.prestosql.spi.eventlistener.EventListenerFactory;
import io.prestosql.spi.security.SystemAccessControlFactory;

import java.util.Arrays;


public class EDSFPlugin implements Plugin {

    @Override
    public Iterable<EventListenerFactory> getEventListenerFactories() {
        System.out.println("Inside EDSF Plugin EventListener");
        return Arrays.asList(new EDSFEventListenerFactory());
    }

    @Override
    public Iterable<SystemAccessControlFactory> getSystemAccessControlFactories() {
        System.out.println("Inside EDSF Plugin SystemAccessControl ==> ");
        return Arrays.asList(new EDSFSystemAccessControlFactory());
    }
}
