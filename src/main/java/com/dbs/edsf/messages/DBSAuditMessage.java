package com.dbs.edsf.messages;

import com.google.gson.GsonBuilder;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.UUID;

public abstract class DBSAuditMessage {
   String user = "";
   List<String> group_list = new LinkedList();
   String ipaddress = "";
   List<String> policy_list = new LinkedList();
   String[] request = new String[]{""};
   String[] modified_request = new String[]{""};
   String time_stamp = (new SimpleDateFormat("yyyy-MM-dd|HH:mm:ss")).format(new Date());
   String database = "";
   String schema = "";
   String audit_type = "";
   String unique_key = UUID.randomUUID().toString();
   String policy_type = "";
   String Effect = "";
   String Action = "";
   String DataDomain = "";

   public String toJsonString() {
      GsonBuilder builder = new GsonBuilder();
      String retVal = builder.create().toJson(this);
      return retVal;
   }
}
