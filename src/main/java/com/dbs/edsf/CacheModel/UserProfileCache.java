package com.dbs.edsf.CacheModel;

import javax.xml.crypto.dsig.keyinfo.KeyValue;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;


/*Col1
      User, List<profile>
private Map<String, Map<String, String>> usersProfileMappingMap;*/


public class UserProfileCache {

    Map<String, Map<String, String>> userProfileMap;
    public Map<String, Map<String, String>> getUserProfileMap() {
        return userProfileMap;
    }

    public void setUserProfileMap(Map<String, Map<String, String>> userProfileMap) {
        this.userProfileMap = userProfileMap;
    }




}
