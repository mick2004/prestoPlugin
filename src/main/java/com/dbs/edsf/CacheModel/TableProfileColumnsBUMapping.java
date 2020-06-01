package com.dbs.edsf.CacheModel;

import java.util.Map;
//class(Map<profile,Map<col,udf>>, String[] BU)
public class TableProfileColumnsBUMapping {



    public Map<String, Map<String,String>> ProfileColumnsMap;
    public String[] BusinessUnit;

    public TableProfileColumnsBUMapping(){

    }
    public TableProfileColumnsBUMapping(Map<String, Map<String, String>> profileColumnsMap, String[] businessUnit) {
        ProfileColumnsMap = profileColumnsMap;
        BusinessUnit = businessUnit;
    }
}
