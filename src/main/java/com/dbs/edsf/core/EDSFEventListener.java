package com.dbs.edsf.core;


import io.prestosql.spi.eventlistener.*;

public class EDSFEventListener implements EventListener{

    @Override
    public void queryCreated(QueryCreatedEvent queryCreatedEvent) {
        //System.out.println("query created event by user  "+queryCreatedEvent.getContext().getUser());
        System.out.println("query created event get query ===="+queryCreatedEvent.getMetadata().getQuery());
        /*System.out.println("query created event get source ===="+queryCreatedEvent.getContext().getSource());
        //System.out.println("query created event get preparedQuery ===="+queryCreatedEvent.getMetadata().getPreparedQuery());
        System.out.println("query created event get QueryState ===="+queryCreatedEvent.getMetadata().getTables());
        //System.out.println("query created event get Payload ===="+queryCreatedEvent.getMetadata().getPayload());
        //System.out.println("query created event get plan ===="+queryCreatedEvent.getMetadata().getPlan());
        System.out.println("query created event get source ===="+queryCreatedEvent.getMetadata());*/


        //System.out.println("getting tableinfo   "+new QueryMetadata().getTables());

    }

    @Override
    public void queryCompleted(QueryCompletedEvent queryCompletedEvent) {
 /*       System.out.println("query completed event by user  "+queryCompletedEvent.getContext().getUser());
        System.out.println("query completed event get query ===="+queryCompletedEvent.getMetadata().getQuery());
        System.out.println("query completed event get query ===="+queryCompletedEvent.getIoMetadata().getInputs().toArray());
        //System.out.println("query completed getinputs  ===="+queryCompletedEvent.getIoMetadata().getInputs().get(0));
        System.out.println("query completed size  ===="+queryCompletedEvent.getIoMetadata().getInputs().size());
        System.out.println("query output metadata  ====="+queryCompletedEvent.getIoMetadata().getOutput());
*/

        for(QueryInputMetadata item:queryCompletedEvent.getIoMetadata().getInputs()){
            System.out.println("tables involved  "+ item.getTable());
            System.out.println("views involved   "+item);
        }

        /*for(QueryOutputMetadata iout:queryCompletedEvent.getIoMetadata().getOutput()){

        }*/



    }








}
