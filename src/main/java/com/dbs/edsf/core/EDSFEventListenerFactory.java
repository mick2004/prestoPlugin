package com.dbs.edsf.core;

import io.prestosql.spi.eventlistener.EventListener;
import io.prestosql.spi.eventlistener.EventListenerFactory;
import java.util.Map;

public class EDSFEventListenerFactory implements EventListenerFactory {
    @Override
    public String getName() {
        return "EDSFEventListener";
    }

    @Override
    public EventListener create(Map<String, String> config) {
        //TODO:Load Config
        return new EDSFEventListener();
    }
}

