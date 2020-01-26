package com.dbs.edsf.presto.authorization.policy;

import io.prestosql.spi.connector.CatalogSchemaName;
import io.prestosql.spi.connector.CatalogSchemaTableName;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.spi.security.AccessDeniedException;
import io.prestosql.spi.security.Identity;
import io.prestosql.sql.tree.Query;
import io.prestosql.sql.tree.Statement;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.net.util.SubnetUtils;

public class DBSAuthUtils {
   private static final String defaultMessage = (new AccessDeniedException("")).getMessage();
   private static final Pattern NAME_PATTERN = Pattern.compile("[a-zA-Z_]([a-zA-Z0-9_:@])*");
   public static final String PARTITIONS_SUFFIX = "$partitions";

   public static boolean startsWith(String str, String prefix, boolean ignoreCase) {
      return prefix.length() > str.length() ? false : str.regionMatches(ignoreCase, 0, prefix, 0, prefix.length());
   }

   public static String formatSql(String query) {
      return (String)Optional.ofNullable(query).filter((str) -> {
         return str.length() != 0;
      }).map((str) -> {
         str = str.trim();
         if (str.endsWith(";")) {
            str = str.substring(0, str.length() - 1);
         }

         return str;
      }).orElse(query);
   }

   public static String getUserName(Identity identity) {
      String user = identity.getUser();
      return getFirstPartInUserName(user);
   }

   public static String getFirstPartInUserName(String name) {
      if (StringUtils.isEmpty(name)) {
         return name;
      } else {
         String user = name;
         String[] parts = name.split("[/@]");
         if (parts.length > 0) {
            user = parts[0];
         }

         return user;
      }
   }

   public static String columnsToString(Optional<Set<String>> columns) {
      StringBuilder sBuilder = new StringBuilder();
      if (columns.isPresent()) {
         Iterator iterator = ((Set)columns.get()).iterator();

         while(iterator.hasNext()) {
            String column = (String)iterator.next();
            sBuilder.append(column);
            if (iterator.hasNext()) {
               sBuilder.append(",");
            }
         }
      }

      return sBuilder.length() > 0 ? sBuilder.toString() : "*";
   }

   public static String getAction(Statement stmt) {
      return stmt instanceof Query ? "Select" : stmt.getClass().getSimpleName();
   }

   public static boolean checkIpInCIDR(String subNetCidr, String ip) {
      if (subNetCidr.contains("/")) {
         SubnetUtils utils = new SubnetUtils(subNetCidr);
         utils.setInclusiveHostCount(true);
         return utils.getInfo().isInRange(ip);
      } else {
         return subNetCidr.equalsIgnoreCase(ip);
      }
   }

   public static CatalogSchemaTableName getHiveParityCatalog(CatalogSchemaTableName catalogSchemaTableName) {
      if (isCatalogInHiveParityList(catalogSchemaTableName.getCatalogName())) {
         SchemaTableName schematableName = catalogSchemaTableName.getSchemaTableName();
         CatalogSchemaTableName catSchemaTabName = new CatalogSchemaTableName(schematableName.getSchemaName(), schematableName.getSchemaName(), schematableName.getTableName());
         return catSchemaTabName;
      } else {
         return catalogSchemaTableName;
      }
   }

   public static CatalogSchemaName getHiveParitySchema(CatalogSchemaName catalogSchemaName) {
      if (isCatalogInHiveParityList(catalogSchemaName.getCatalogName())) {
         CatalogSchemaName catSchemaName = new CatalogSchemaName(catalogSchemaName.getSchemaName(), catalogSchemaName.getSchemaName());
         return catSchemaName;
      } else {
         return catalogSchemaName;
      }
   }

   public static boolean isCatalogInHiveParityList(String catalogName) {
      Optional<List<String>> hiveParityCatalogs = Optional.ofNullable(DBSPrestoConfiguration.getInstance().getTrimmedVarList(DBSPrestoConfiguration.DBSConfVars.DBS_PRESTO_PLUGIN_CATALOGS_HS2_PARITY_LIST, ","));
//      Optional<Boolean> exists = hiveParityCatalogs.map((catalogs) -> {
//         Stream var10000 = catalogs.stream();
//         catalogName.getClass();
//         return var10000.anyMatch(catalogName::equalsIgnoreCase);
//      });
      //return (Boolean)exists.orElse(false);

      return false;
   }

   public static String toString(CatalogSchemaName catalogSchemaName) {
      return catalogSchemaName.getCatalogName() + '.' + catalogSchemaName.getSchemaName();
   }

   public static String toString(SchemaTableName schemaTableName) {
      return schemaTableName.getSchemaName() + '.' + schemaTableName.getTableName();
   }

   public static String toString(CatalogSchemaTableName catalogSchemaTableName) {
      return catalogSchemaTableName.getCatalogName() + '.' + toString(catalogSchemaTableName.getSchemaTableName());
   }

   public static String formatExceptionMessage(AccessDeniedException ade) {
      return ade.getMessage().replaceFirst(defaultMessage, "");
   }

   public static String formatToStringWithDelimiters(CatalogSchemaTableName catalogSchemaTableName) {
      List<String> parts = new LinkedList();
      parts.add(catalogSchemaTableName.getCatalogName());
      parts.add(catalogSchemaTableName.getSchemaTableName().getSchemaName());
      parts.add(catalogSchemaTableName.getSchemaTableName().getTableName());
      return (String)parts.stream().map(DBSAuthUtils::handleDelimiters).collect(Collectors.joining("."));
   }

   private static String handleDelimiters(String name) {
      return NAME_PATTERN.matcher(name).matches() ? name : "\"" + name + "\"";
   }
}
