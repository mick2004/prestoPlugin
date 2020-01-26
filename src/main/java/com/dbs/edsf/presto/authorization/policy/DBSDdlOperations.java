package com.dbs.edsf.presto.authorization.policy;

public enum DBSDdlOperations {
   CREATE_SCHEMA("CREATE SCHEMA"),
   CREATE_VIEW("CREATE VIEW"),
   CREATE_TABLE("CREATE TABLE"),
   DROP_SCHEMA("DROP SCHEMA"),
   DROP_TABLE("DROP TABLE"),
   DROP_VIEW("DROP VIEW"),
   ALTER_SCHEMA("ALTER SCHEMA"),
   ALTER_TABLE("ALTER TABLE"),
   INSERT_INTO_TABLE("INSERT INTO"),
   DELETE_FROM_TABLE("DELETE FROM");

   private String sqlCommand;

   private DBSDdlOperations(String command) {
      this.sqlCommand = command;
   }

   public String getValue() {
      return this.sqlCommand;
   }

   public String getQuery(String ddlElement) {
      StringBuilder query = new StringBuilder("");
      query.append(this.sqlCommand).append(" ").append(ddlElement);
      if (this.sqlCommand.equalsIgnoreCase(INSERT_INTO_TABLE.sqlCommand)) {
         query.append(" VALUES('x','xx')");
      }

      return query.toString();
   }
}
