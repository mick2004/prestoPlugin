package com.dbs.edsf.core;


import io.prestosql.spi.eventlistener.EventListener;
import io.prestosql.spi.eventlistener.QueryCompletedEvent;
import io.prestosql.spi.eventlistener.QueryCreatedEvent;
import io.prestosql.spi.eventlistener.ColumnInfo;
import io.prestosql.spi.eventlistener.TableInfo;

public class EDSFEventListener implements EventListener{

    @Override
    public void queryCreated(QueryCreatedEvent queryCreatedEvent) {
        System.out.println("query created event by user  "+queryCreatedEvent.getContext().getUser());
        System.out.println("query created event get query ===="+queryCreatedEvent.getMetadata().getQuery());
        System.out.println("query created event get source ===="+queryCreatedEvent.getContext().getSource());
        //System.out.println("query created event get preparedQuery ===="+queryCreatedEvent.getMetadata().getPreparedQuery());
        System.out.println("query created event get QueryState ===="+queryCreatedEvent.getMetadata().getQueryState());
        //System.out.println("query created event get Payload ===="+queryCreatedEvent.getMetadata().getPayload());
        //System.out.println("query created event get plan ===="+queryCreatedEvent.getMetadata().getPlan());
        System.out.println("query created event get source ===="+queryCreatedEvent.getContext());
        //System.out.println("getting tableinfo   "+new QueryMetadata().getTables());

    }

    @Override
    public void queryCompleted(QueryCompletedEvent queryCompletedEvent) {
        System.out.println("query completed event by user  "+queryCompletedEvent.getContext().getUser());
        System.out.println("query completed event get query ===="+queryCompletedEvent.getMetadata().getQuery());

    }








}
