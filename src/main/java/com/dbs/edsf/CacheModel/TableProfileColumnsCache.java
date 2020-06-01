package com.dbs.edsf.CacheModel;

import java.util.Map;
//Map<Table, class(Map<profile,Map<col,udf>>, String[] BU)>
public class TableProfileColumnsCache {
    public Map<String, TableProfileColumnsBUMapping> getUserTableProfilemap() {
        return userTableProfilemap;
    }

    public void setUserTableProfilemap(Map<String, TableProfileColumnsBUMapping> userTableProfilemap) {
        this.userTableProfilemap = userTableProfilemap;
    }

    Map<String, TableProfileColumnsBUMapping> userTableProfilemap;

    public TableProfileColumnsCache(){

    }
    public TableProfileColumnsCache(Map<String, TableProfileColumnsBUMapping> userTableProfilemap) {
        this.userTableProfilemap = userTableProfilemap;
    }


}
