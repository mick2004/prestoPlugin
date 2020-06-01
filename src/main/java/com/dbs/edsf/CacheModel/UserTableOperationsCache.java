package com.dbs.edsf.CacheModel;

import java.util.Map;

/*HashMap<String, HashMap<String, UserTableMapping>> hm = new HashMap<>();
        String1 (outer key)= user
        String2 (inner key)= table
        Class = anchorpoints(row level filter),operation(select,delete)*/

public class UserTableOperationsCache {
    Map<String, Map<String, UserOperationRLFMapping>> hm;

    public Map<String, Map<String, UserOperationRLFMapping>> getHm() {
        return hm;
    }

    public void setHm(Map<String, Map<String, UserOperationRLFMapping>> hm) {
        this.hm = hm;
    }


}
