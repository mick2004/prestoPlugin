package com.dbs.edsf.presto.authorization.ast;

import io.prestosql.sql.tree.Node;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;

public class DBSAstVisitorScope {
   private Node parentNode;
   private Optional<DBSAstVisitorScope> parentScope;
   private Optional<List<String>> withViews = Optional.of(new LinkedList());

   public DBSAstVisitorScope(Node node) {
      this.parentNode = node;
      this.parentScope = Optional.empty();
   }

   public DBSAstVisitorScope(Node node, Optional<DBSAstVisitorScope> parentScope) {
      this.parentNode = node;
      this.parentScope = parentScope;
   }

   public Node getParentNode() {
      return this.parentNode;
   }

   public void setParentNode(Node parentNode) {
      this.parentNode = parentNode;
   }

   public Optional<DBSAstVisitorScope> getParentScope() {
      return this.parentScope;
   }

   public void setParentScope(Optional<DBSAstVisitorScope> parentScope) {
      this.parentScope = parentScope;
   }

   public Optional<String> getViewName(String tableName) {
      return this.withViews.flatMap((viewslist) -> {
         return viewslist.stream().filter((view) -> {
            return view.equalsIgnoreCase(tableName);
         }).findFirst();
      });
   }

   public void addViewName(String viewName) {
      if (viewName != null && !viewName.isEmpty()) {
         this.withViews.ifPresent((list) -> {
            list.add(viewName);
         });
      }
   }
}
