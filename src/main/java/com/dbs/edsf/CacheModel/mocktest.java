package com.dbs.edsf.CacheModel;

import java.util.Map;

public class mocktest {


    public static void main(String[] args){
        CacheMockData cacheMockData = new CacheMockData();

        UserProfileCache userProfileCache =  new UserProfileCache();
        userProfileCache = cacheMockData.UserProfileCacheLoad();

        Map<String, Map<String,String>> upMap= userProfileCache.getUserProfileMap();
        System.out.println(upMap);
        Boolean profile =upMap.containsKey("nishant");
        System.out.println("whether user exists in profile or not"+profile);
        System.out.println("user bu level access "+upMap.get("nishant"));

        UserTableOperationsCache userTableOperationsCache = new UserTableOperationsCache();
        userTableOperationsCache = cacheMockData.UserTableOperationsCacheLoad();
        Map<String, Map<String, UserOperationRLFMapping>> uTrlfMap = userTableOperationsCache.getHm();
        System.out.println("userTableOperationsCache.getHm();"+userTableOperationsCache.getHm());
        System.out.println("table present "+uTrlfMap.get("nishant").containsKey("tableb"));
        System.out.println("operation permitted for user "+uTrlfMap.get("nishant").get("tableb").operation);
        System.out.println("rowLevel filter columns "+uTrlfMap.get("nishant").get("tableb").rowLevelFilter);

        TableProfileColumnsCache tableProfileColumnsCache = new TableProfileColumnsCache();
        tableProfileColumnsCache = cacheMockData.tableProfileColumnsCacheLoad();
        System.out.println(tableProfileColumnsCache.getUserTableProfilemap().get("tableb").BusinessUnit);
        System.out.println(tableProfileColumnsCache.getUserTableProfilemap().get("tablea").ProfileColumnsMap.get("all"));
        System.out.println(tableProfileColumnsCache.getUserTableProfilemap().get("tablea").BusinessUnit);


/*        System.out.println(tableProfileColumnsCache.getUserTableProfilemap().get("tableB").BusinessUnit);
        System.out.println(tableProfileColumnsCache.getUserTableProfilemap().get("tableB").ProfileColumnsMap);*/



    }
}
