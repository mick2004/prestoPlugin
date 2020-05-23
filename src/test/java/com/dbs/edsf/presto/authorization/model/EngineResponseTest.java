/*
package com.dbs.edsf.presto.authorization.model;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.testng.Assert.*;

public class EngineResponseTest {

    @Test
    public void testObjToJson() {

        EngineRequest re=new EngineRequest("abhishekps", Arrays.asList( new String[]{ "table1","table2","table3" }));

        String req ="";
        try {
            req=re.objToJson();
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
        System.out.println(req);


        List<Column> cols1= Arrays.asList(new Column[]{
                new Column("col1","")
                ,new Column("col2","col2UnProtect")
                ,new Column("col3","")
        });

      List<Column> cols2= Arrays.asList(new Column[]{
                new Column("id","")
                ,new Column("name","nameUnProtect")
        });

        List<Column> cols3= Arrays.asList(new Column[]{
                new Column("id","")
                ,new Column("desc","descUnProtect")
        });

        Table t1=new Table("userdb.table1","table1",cols1,"col1=5");
        Table t2=new Table("userdb.table2","table2",cols1,"id=5");
        Table t3=new Table("userdb.table3","table3",cols1,"id=5");

       EngineResponse er=new EngineResponse("abhishekps","SUCCESS","ALLOW",
                Arrays.asList( new Table[]{ t1,t2,t3 }));

        String req1 ="";
        try {
            req1=er.objToJson();
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
        System.out.println(req1);


    }
}
*/
