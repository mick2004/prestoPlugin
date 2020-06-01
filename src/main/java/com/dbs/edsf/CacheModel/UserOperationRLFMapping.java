package com.dbs.edsf.CacheModel;

import java.util.List;

public class UserOperationRLFMapping {
    public List<String> operation;//select,delete,insert
    public List<String> rowLevelFilter; //IN,EDSF
    public UserOperationRLFMapping(){

    }
    public UserOperationRLFMapping(List<String> operation, List<String> rowLevelFilter) {
        this.operation = operation;
        this.rowLevelFilter = rowLevelFilter;
    }


}
