package com.dbs.edsf.presto.authorization.ast;

import com.dbs.edsf.presto.authorization.DBSSystemAccessControl;
import io.prestosql.metadata.QualifiedObjectName;
import io.prestosql.spi.StatementRewriteContext;
import io.prestosql.sql.SqlFormatter;
import io.prestosql.sql.parser.ParsingOptions;
import io.prestosql.sql.parser.SqlParser;
import io.prestosql.sql.parser.ParsingOptions.DecimalLiteralTreatment;
import io.prestosql.sql.tree.AliasedRelation;
import io.prestosql.sql.tree.AllColumns;
import io.prestosql.sql.tree.ArithmeticBinaryExpression;
import io.prestosql.sql.tree.ArithmeticUnaryExpression;
import io.prestosql.sql.tree.ArrayConstructor;
import io.prestosql.sql.tree.BetweenPredicate;
import io.prestosql.sql.tree.BindExpression;
import io.prestosql.sql.tree.Cast;
import io.prestosql.sql.tree.CoalesceExpression;
import io.prestosql.sql.tree.ComparisonExpression;
import io.prestosql.sql.tree.DereferenceExpression;
import io.prestosql.sql.tree.Except;
import io.prestosql.sql.tree.ExistsPredicate;
import io.prestosql.sql.tree.Explain;
import io.prestosql.sql.tree.Expression;
import io.prestosql.sql.tree.Extract;
import io.prestosql.sql.tree.FunctionCall;
import io.prestosql.sql.tree.Identifier;
import io.prestosql.sql.tree.IfExpression;
import io.prestosql.sql.tree.InListExpression;
import io.prestosql.sql.tree.InPredicate;
import io.prestosql.sql.tree.Insert;
import io.prestosql.sql.tree.Intersect;
import io.prestosql.sql.tree.IsNotNullPredicate;
import io.prestosql.sql.tree.IsNullPredicate;
import io.prestosql.sql.tree.Join;
import io.prestosql.sql.tree.LambdaExpression;
import io.prestosql.sql.tree.Lateral;
import io.prestosql.sql.tree.LikePredicate;
import io.prestosql.sql.tree.LogicalBinaryExpression;
import io.prestosql.sql.tree.Node;
import io.prestosql.sql.tree.NodeLocation;
import io.prestosql.sql.tree.NotExpression;
import io.prestosql.sql.tree.NullIfExpression;
import io.prestosql.sql.tree.QualifiedName;
import io.prestosql.sql.tree.QuantifiedComparisonExpression;
import io.prestosql.sql.tree.Query;
import io.prestosql.sql.tree.QueryBody;
import io.prestosql.sql.tree.QuerySpecification;
import io.prestosql.sql.tree.Relation;
import io.prestosql.sql.tree.SampledRelation;
import io.prestosql.sql.tree.SearchedCaseExpression;
import io.prestosql.sql.tree.Select;
import io.prestosql.sql.tree.SelectItem;
import io.prestosql.sql.tree.SetOperation;
import io.prestosql.sql.tree.SimpleCaseExpression;
import io.prestosql.sql.tree.SingleColumn;
import io.prestosql.sql.tree.Statement;
import io.prestosql.sql.tree.SubqueryExpression;
import io.prestosql.sql.tree.SubscriptExpression;
import io.prestosql.sql.tree.Table;
import io.prestosql.sql.tree.TableSubquery;
import io.prestosql.sql.tree.TryExpression;
import io.prestosql.sql.tree.Union;
import io.prestosql.sql.tree.Unnest;
import io.prestosql.sql.tree.Values;
import io.prestosql.sql.tree.WhenClause;
import io.prestosql.sql.tree.With;
import io.prestosql.sql.tree.WithQuery;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DBSRowFilterColumnMaskRewrite implements QueryRewrite {
   private static final Logger log = LoggerFactory.getLogger(DBSRowFilterColumnMaskRewrite.class);

   public String rewrite(StatementRewriteContext session, SqlParser parser, Statement node, DBSSystemAccessControl accessControl) {
      DBSRowFilterColumnMaskRewrite.RewriteVisitor visitor = new DBSRowFilterColumnMaskRewrite.RewriteVisitor(parser, session, accessControl);
      return visitor.rewrite(node);
   }

   private static class RewriteVisitor extends DBSAstVisitor {
      private final StatementRewriteContext session;
      private final SqlParser sqlParser;
      private final DBSSystemAccessControl accessControl;
      private DBSAstVisitorScope rootContext;
      private ParsingOptions parsingOptions;
      private DBSTableKeeper DBSTableKeeper;

      public RewriteVisitor(SqlParser sqlParser, StatementRewriteContext session, DBSSystemAccessControl accessControl) {
         super(session, accessControl);
         this.sqlParser = (SqlParser)Objects.requireNonNull(sqlParser, "sqlParser is null");
         this.session = (StatementRewriteContext)Objects.requireNonNull(session, "session is null");
         this.accessControl = (DBSSystemAccessControl)Objects.requireNonNull(accessControl, "accessControl is null");
         this.parsingOptions = new ParsingOptions(DecimalLiteralTreatment.AS_DECIMAL);
         this.DBSTableKeeper = new DBSTableKeeper(session, accessControl.getPolicyHandler());
      }

      public String rewrite(Node node) {
         Statement modifiedStmt = (Statement)this.process(node);
         if (DBSRowFilterColumnMaskRewrite.log.isDebugEnabled()) {
            DBSRowFilterColumnMaskRewrite.log.debug("Modified query statement:" + modifiedStmt);
         }

         String modifiedQuery = SqlFormatter.formatSql(modifiedStmt, Optional.empty());
         modifiedQuery = modifiedQuery.replaceAll("\\n", " ");
         modifiedQuery = this.DBSTableKeeper.replacePlaceHolders(modifiedQuery);
         return modifiedQuery;
      }

      public Node process(Node node) {
         this.rootContext = new DBSAstVisitorScope(node);
         return (Node)super.process(node, this.rootContext);
      }

      protected Node visitExplain(Explain node, DBSAstVisitorScope context) {
         if (node.getStatement() instanceof Query) {
            Query query = (Query)this.visitQuery((Query)node.getStatement(), context);
            return node.getLocation().isPresent() ? new Explain(query, node.isAnalyze(), node.isVerbose(), node.getOptions()) : new Explain((NodeLocation)node.getLocation().get(), node.isAnalyze(), node.isVerbose(), query, node.getOptions());
         } else {
            return this.visitNode(node, context);
         }
      }

      protected Node visitSelect(Select node, DBSAstVisitorScope context) {
         List<SelectItem> selectItems = node.getSelectItems();
         List<SelectItem> modifiedItems = (List)selectItems.stream().map((selectItem) -> {
            return (SelectItem)this.visitSelectItem(selectItem, context);
         }).collect(Collectors.toList());
         return (Node)node.getLocation().map((nodeLocation) -> {
            return new Select(nodeLocation, node.isDistinct(), modifiedItems);
         }).orElse(new Select(node.isDistinct(), modifiedItems));
      }

      protected Node visitSelectItem(SelectItem node, DBSAstVisitorScope context) {
         if (node instanceof SingleColumn) {
            SingleColumn singleColumn = (SingleColumn)node;
            Expression expression = (Expression)this.visitExpression(singleColumn.getExpression(), context);
            return (Node)expression.getLocation().map((nodeLocation) -> {
               return new SingleColumn(nodeLocation, expression, singleColumn.getAlias());
            }).orElse(new SingleColumn(expression, singleColumn.getAlias()));
         } else {
            if (node instanceof AllColumns) {
               AllColumns allColumns = (AllColumns)node;
               Optional<QualifiedName> prefix = allColumns.getPrefix();
               if (prefix.isPresent()) {
                  List<String> parts = ((QualifiedName)prefix.get()).getParts();
                  if (parts.size() > 1) {
                     QualifiedName modifiedQualifiedName = QualifiedName.of((String)parts.get(parts.size() - 1));
                     return (Node)allColumns.getLocation().map((nodeLocation) -> {
                        return new AllColumns(nodeLocation, modifiedQualifiedName);
                     }).orElse(new AllColumns(modifiedQualifiedName));
                  }
               }
            }

            return node;
         }
      }

      protected Node visitSubqueryExpression(SubqueryExpression node, DBSAstVisitorScope context) {
         Query query = (Query)this.visitQuery(node.getQuery(), context);
         SubqueryExpression subqueryExpression = (SubqueryExpression)node.getLocation().map((nodeLocation) -> {
            return new SubqueryExpression(nodeLocation, query);
         }).orElse(new SubqueryExpression(query));
         return subqueryExpression;
      }

      protected Node visitQueryBody(QueryBody node, DBSAstVisitorScope context) {
         return (Node)node.accept(this, context);
      }

      protected Node visitRelation(Relation node, DBSAstVisitorScope context) {
         return (Node)node.accept(this, context);
      }

      protected Node visitExpression(Expression node, DBSAstVisitorScope context) {
         DBSRowFilterColumnMaskRewrite.log.debug("expression type:{}", node.getClass().getSimpleName());
         if (node instanceof SubqueryExpression) {
            return this.visitSubqueryExpression((SubqueryExpression)node, context);
         } else {
            List bind;
            if (node instanceof DereferenceExpression) {
               DereferenceExpression dereferenceExpression = (DereferenceExpression)node;
               QualifiedName qualifiedName = DereferenceExpression.getQualifiedName(dereferenceExpression);
               if (qualifiedName != null) {
                  bind = qualifiedName.getParts();
                  if (bind.size() > 2) {
                     QualifiedName modifiedName = QualifiedName.of((String)bind.get(bind.size() - 2));
                     return (Node)dereferenceExpression.getLocation().map((nodeLocation) -> {
                        return new DereferenceExpression(nodeLocation, DereferenceExpression.from(modifiedName), dereferenceExpression.getField());
                     }).orElse(new DereferenceExpression(DereferenceExpression.from(modifiedName), dereferenceExpression.getField()));
                  }
               }

               return node;
            } else {
               Expression function;
               Expression index;
               if (node instanceof InPredicate) {
                  InPredicate inPredicate = (InPredicate)node;
                  function = (Expression)this.visitExpression(inPredicate.getValue(), context);
                  index = (Expression)this.visitExpression(inPredicate.getValueList(), context);
                  return (Node)node.getLocation().map((nodeLocation) -> {
                     return new InPredicate(nodeLocation, function, index);
                  }).orElse(new InPredicate(function, index));
               } else if (node instanceof IfExpression) {
                  IfExpression ifExpression = (IfExpression)node;
                  function = (Expression)this.visitExpression(ifExpression.getCondition(), context);
                  index = (Expression)this.visitExpression(ifExpression.getTrueValue(), context);
                  Expression falseValueExpression = (Expression)ifExpression.getFalseValue().map((expression) -> {
                     return (Expression)this.visitExpression(expression, context);
                  }).orElse(null);
                  return (Node)ifExpression.getLocation().map((nodeLocation) -> {
                     return new IfExpression(nodeLocation, function, index, falseValueExpression);
                  }).orElse(new IfExpression(function, index, falseValueExpression));
               } else if (node instanceof QuantifiedComparisonExpression) {
                  QuantifiedComparisonExpression quantifiedComparisonExpression = (QuantifiedComparisonExpression)node;
                  function = (Expression)this.visitExpression(quantifiedComparisonExpression.getValue(), context);
                  index = (Expression)this.visitExpression(quantifiedComparisonExpression.getSubquery(), context);
                  return (Node)quantifiedComparisonExpression.getLocation().map((nodeLocation) -> {
                     return new QuantifiedComparisonExpression(nodeLocation, quantifiedComparisonExpression.getOperator(), quantifiedComparisonExpression.getQuantifier(), function, index);
                  }).orElse(new QuantifiedComparisonExpression(quantifiedComparisonExpression.getOperator(), quantifiedComparisonExpression.getQuantifier(), function, index));
               } else if (node instanceof ExistsPredicate) {
                  ExistsPredicate existsPredicate = (ExistsPredicate)node;
                  function = (Expression)this.visitExpression(existsPredicate.getSubquery(), context);
                  return (Node)existsPredicate.getLocation().map((nodeLocation) -> {
                     return new ExistsPredicate(nodeLocation, function);
                  }).orElse(new ExistsPredicate(function));
               } else if (node instanceof NotExpression) {
                  NotExpression notExpression = (NotExpression)node;
                  function = (Expression)this.visitExpression(notExpression.getValue(), context);
                  return (Node)notExpression.getLocation().map((nodeLocation) -> {
                     return new NotExpression(nodeLocation, function);
                  }).orElse(new NotExpression(function));
               } else if (node instanceof NullIfExpression) {
                  NullIfExpression nullIfExpression = (NullIfExpression)node;
                  function = (Expression)this.visitExpression(nullIfExpression.getFirst(), context);
                  index = (Expression)this.visitExpression(nullIfExpression.getSecond(), context);
                  return (Node)nullIfExpression.getLocation().map((nodeLocation) -> {
                     return new NullIfExpression(nodeLocation, function, index);
                  }).orElse(new NullIfExpression(function, index));
               } else if (node instanceof Cast) {
                  Cast cast = (Cast)node;
                  function = (Expression)this.visitExpression(cast.getExpression(), context);
                  return (Node)cast.getLocation().map((nodeLocation) -> {
                     return new Cast(nodeLocation, function, cast.getType(), cast.isSafe(), cast.isTypeOnly());
                  }).orElse(new Cast(function, cast.getType(), cast.isSafe(), cast.isTypeOnly()));
               } else {
                  List modifiedExpressions;
                  if (node instanceof CoalesceExpression) {
                     CoalesceExpression coalesceExpression = (CoalesceExpression)node;
                     modifiedExpressions = (List)coalesceExpression.getOperands().stream().map((expression) -> {
                        return (Expression)this.visitExpression(expression, context);
                     }).collect(Collectors.toList());
                     return (Node)coalesceExpression.getLocation().map((nodeLocation) -> {
                        return new CoalesceExpression(nodeLocation, modifiedExpressions);
                     }).orElse(new CoalesceExpression(modifiedExpressions));
                  } else if (node instanceof ComparisonExpression) {
                     ComparisonExpression comparisonExpression = (ComparisonExpression)node;
                     function = (Expression)this.visitExpression(comparisonExpression.getLeft(), context);
                     index = (Expression)this.visitExpression(comparisonExpression.getRight(), context);
                     return (Node)comparisonExpression.getLocation().map((nodeLocation) -> {
                        return new ComparisonExpression(nodeLocation, comparisonExpression.getOperator(), function, index);
                     }).orElse(new ComparisonExpression(comparisonExpression.getOperator(), function, index));
                  } else {
                     Optional defaultExpression;
                     if (node instanceof FunctionCall) {
                        FunctionCall functionCall = (FunctionCall)node;
                        modifiedExpressions = functionCall.getArguments();
                        bind = (List)modifiedExpressions.stream().map((expression) -> {
                           return (Expression)this.visitExpression((Expression) expression, context);
                        }).collect(Collectors.toList());
                        defaultExpression = functionCall.getFilter().map((expression) -> {
                           return (Expression)this.visitExpression(expression, context);
                        });
                        return (Node)functionCall.getLocation().map((nodeLocation) -> {
                           return new FunctionCall(nodeLocation, functionCall.getName(), functionCall.getWindow(), defaultExpression, functionCall.getOrderBy(), functionCall.isDistinct(), bind);
                        }).orElse(new FunctionCall(functionCall.getName(), functionCall.getWindow(), defaultExpression, functionCall.getOrderBy(), functionCall.isDistinct(), bind));
                     } else if (node instanceof LogicalBinaryExpression) {
                        LogicalBinaryExpression logicalBinaryExpression = (LogicalBinaryExpression)node;
                        function = (Expression)this.visitExpression(logicalBinaryExpression.getLeft(), context);
                        index = (Expression)this.visitExpression(logicalBinaryExpression.getRight(), context);
                        return (Node)logicalBinaryExpression.getLocation().map((nodeLocation) -> {
                           return new LogicalBinaryExpression(nodeLocation, logicalBinaryExpression.getOperator(), function, index);
                        }).orElse(new LogicalBinaryExpression(logicalBinaryExpression.getOperator(), function, index));
                     } else if (node instanceof ArithmeticBinaryExpression) {
                        ArithmeticBinaryExpression arithmeticBinaryExpression = (ArithmeticBinaryExpression)node;
                        function = (Expression)this.visitExpression(arithmeticBinaryExpression.getLeft(), context);
                        index = (Expression)this.visitExpression(arithmeticBinaryExpression.getRight(), context);
                        return (Node)arithmeticBinaryExpression.getLocation().map((nodeLocation) -> {
                           return new ArithmeticBinaryExpression(nodeLocation, arithmeticBinaryExpression.getOperator(), function, index);
                        }).orElse(new ArithmeticBinaryExpression(arithmeticBinaryExpression.getOperator(), function, index));
                     } else if (node instanceof ArithmeticUnaryExpression) {
                        ArithmeticUnaryExpression arithmeticUnaryExpression = (ArithmeticUnaryExpression)node;
                        function = (Expression)this.visitExpression(arithmeticUnaryExpression.getValue(), context);
                        return (Node)arithmeticUnaryExpression.getLocation().map((nodeLocation) -> {
                           return new ArithmeticUnaryExpression(nodeLocation, arithmeticUnaryExpression.getSign(), function);
                        }).orElse(new ArithmeticUnaryExpression(arithmeticUnaryExpression.getSign(), function));
                     } else if (node instanceof SimpleCaseExpression) {
                        SimpleCaseExpression simpleCaseExpression = (SimpleCaseExpression)node;
                        function = (Expression)this.visitExpression(simpleCaseExpression.getOperand(), context);
                        bind = (List)simpleCaseExpression.getWhenClauses().stream().map((whenclausex) -> {
                           return (WhenClause)this.visitExpression(whenclausex, (DBSAstVisitorScope)context);
                        }).collect(Collectors.toList());
                        defaultExpression = simpleCaseExpression.getDefaultValue().map((expression) -> {
                           return (Expression)this.visitExpression(expression, context);
                        });
                        return (Node)simpleCaseExpression.getLocation().map((nodeLocation) -> {
                           return new SimpleCaseExpression(nodeLocation, function, bind, defaultExpression);
                        }).orElse(new SimpleCaseExpression(function, bind, defaultExpression));
                     } else if (node instanceof SearchedCaseExpression) {
                        SearchedCaseExpression searchedCaseExpression = (SearchedCaseExpression)node;
                        modifiedExpressions = (List)searchedCaseExpression.getWhenClauses().stream().map((whenclausex) -> {
                           return (WhenClause)this.visitExpression(whenclausex, (DBSAstVisitorScope)context);
                        }).collect(Collectors.toList());
                         defaultExpression = searchedCaseExpression.getDefaultValue().map((expression) -> {
                           return (Expression)this.visitExpression(expression, context);
                        });
                        return (Node)searchedCaseExpression.getLocation().map((nodeLocation) -> {
                           return new SearchedCaseExpression(nodeLocation, modifiedExpressions, defaultExpression);
                        }).orElse(new SearchedCaseExpression(modifiedExpressions, defaultExpression));
                     } else if (node instanceof WhenClause) {
                        WhenClause whenclause = (WhenClause)node;
                        function = (Expression)this.visitExpression(whenclause.getOperand(), context);
                        index = (Expression)this.visitExpression(whenclause.getResult(), context);
                        return (Node)whenclause.getLocation().map((nodeLocation) -> {
                           return new WhenClause(nodeLocation, function, index);
                        }).orElse(new WhenClause(function, index));
                     } else if (node instanceof SubscriptExpression) {
                        SubscriptExpression subscriptExpression = (SubscriptExpression)node;
                        function = (Expression)this.visitExpression(subscriptExpression.getBase(), context);
                        index = (Expression)this.visitExpression(subscriptExpression.getIndex(), context);
                        return (Node)subscriptExpression.getLocation().map((nodeLocation) -> {
                           return new SubscriptExpression(nodeLocation, function, index);
                        }).orElse(new SubscriptExpression(function, index));
                     } else if (node instanceof InListExpression) {
                        InListExpression inListExpression = (InListExpression)node;
                        modifiedExpressions = (List)inListExpression.getValues().stream().map((expression) -> {
                           return (Expression)this.visitExpression(expression, context);
                        }).collect(Collectors.toList());
                        return (Node)inListExpression.getLocation().map((nodeLocation) -> {
                           return new InListExpression(nodeLocation, modifiedExpressions);
                        }).orElse(new InListExpression(modifiedExpressions));
                     } else if (node instanceof TryExpression) {
                        TryExpression tryExpression = (TryExpression)node;
                        function = (Expression)this.visitExpression(tryExpression.getInnerExpression(), context);
                        return (Node)tryExpression.getLocation().map((nodeLocation) -> {
                           return new TryExpression(nodeLocation, function);
                        }).orElse(new TryExpression(function));
                     } else if (node instanceof Extract) {
                        Extract extract = (Extract)node;
                        function = (Expression)this.visitExpression(extract.getExpression(), context);
                        return (Node)extract.getLocation().map((nodeLocation) -> {
                           return new Extract(nodeLocation, function, extract.getField());
                        }).orElse(new Extract(function, extract.getField()));
                     } else if (node instanceof LikePredicate) {
                        LikePredicate likePredicate = (LikePredicate)node;
                        function = (Expression)this.visitExpression(likePredicate.getValue(), context);
                        return (Node)likePredicate.getLocation().map((nodeLocation) -> {
                           return new LikePredicate(nodeLocation, function, likePredicate.getPattern(), likePredicate.getEscape());
                        }).orElse(new LikePredicate(function, likePredicate.getPattern(), likePredicate.getEscape()));
                     } else if (node instanceof IsNullPredicate) {
                        IsNullPredicate isNullPredicate = (IsNullPredicate)node;
                        function = (Expression)this.visitExpression(isNullPredicate.getValue(), context);
                        return (Node)isNullPredicate.getLocation().map((nodeLocation) -> {
                           return new IsNullPredicate(nodeLocation, function);
                        }).orElse(new IsNullPredicate(function));
                     } else if (node instanceof IsNotNullPredicate) {
                        IsNotNullPredicate isNotNullPredicate = (IsNotNullPredicate)node;
                        function = (Expression)this.visitExpression(isNotNullPredicate.getValue(), context);
                        return (Node)isNotNullPredicate.getLocation().map((nodeLocation) -> {
                           return new IsNotNullPredicate(nodeLocation, function);
                        }).orElse(new IsNotNullPredicate(function));
                     } else if (node instanceof BetweenPredicate) {
                        BetweenPredicate betweenPredicate = (BetweenPredicate)node;
                        function = (Expression)this.visitExpression(betweenPredicate.getValue(), context);
                        return (Node)betweenPredicate.getLocation().map((nodeLocation) -> {
                           return new BetweenPredicate(nodeLocation, function, betweenPredicate.getMin(), betweenPredicate.getMax());
                        }).orElse(new BetweenPredicate(function, betweenPredicate.getMin(), betweenPredicate.getMax()));
                     } else if (node instanceof ArrayConstructor) {
                        ArrayConstructor arrayConstructor = (ArrayConstructor)node;
                        modifiedExpressions = (List)arrayConstructor.getValues().stream().map((expression) -> {
                           return (Expression)this.visitExpression(expression, context);
                        }).collect(Collectors.toList());
                        return (Node)arrayConstructor.getLocation().map((nodeLocation) -> {
                           return new ArrayConstructor(nodeLocation, modifiedExpressions);
                        }).orElse(new ArrayConstructor(modifiedExpressions));
                     } else if (node instanceof LambdaExpression) {
                        LambdaExpression lambdaExpression = (LambdaExpression)node;
                        function = (Expression)this.visitExpression(lambdaExpression.getBody(), context);
                        return (Node)lambdaExpression.getLocation().map((nodeLocation) -> {
                           return new LambdaExpression(nodeLocation, lambdaExpression.getArguments(), function);
                        }).orElse(new LambdaExpression(lambdaExpression.getArguments(), function));
                     } else if (node instanceof BindExpression) {
                        BindExpression bindExpression = (BindExpression)node;
                        function = (Expression)this.visitExpression(bindExpression.getFunction(), context);
                        bind = (List)bindExpression.getValues().stream().map((expression) -> {
                           return (Expression)this.visitExpression(expression, context);
                        }).collect(Collectors.toList());
                        return (Node)bindExpression.getLocation().map((nodeLocation) -> {
                           return new BindExpression(nodeLocation, bind, function);
                        }).orElse(new BindExpression(bind, function));
                     } else {
                        return this.visitNode(node, context);
                     }
                  }
               }
            }
         }
      }

      protected Node visitWith(With node, DBSAstVisitorScope context) {
         List<WithQuery> withQueries = new ArrayList();
         Iterator var4 = node.getQueries().iterator();

         while(var4.hasNext()) {
            WithQuery withQuery = (WithQuery)var4.next();
            withQueries.add((WithQuery)this.visitWithQuery(withQuery, context));
         }

         return (Node)node.getLocation().map((nodeLocation) -> {
            return new With(nodeLocation, node.isRecursive(), withQueries);
         }).orElse(new With(node.isRecursive(), withQueries));
      }

      protected Node visitWithQuery(WithQuery node, DBSAstVisitorScope context) {
         Query query = (Query)this.visitQuery(node.getQuery(), context);
         this.rootContext.addViewName(node.getName().getValue());
         return (Node)node.getLocation().map((nodeLocation) -> {
            return new WithQuery(nodeLocation, node.getName(), query, node.getColumnNames());
         }).orElse(new WithQuery(node.getName(), query, node.getColumnNames()));
      }

      protected Node visitLateral(Lateral lateral, DBSAstVisitorScope context) {
         Query query = (Query)this.visitQuery(lateral.getQuery(), context);
         return (Node)lateral.getLocation().map((nodeLocation) -> {
            return new Lateral((NodeLocation)query.getLocation().get(), query);
         }).orElse(new Lateral(query));
      }

      protected Node visitIntersect(Intersect intersect, DBSAstVisitorScope context) {
         List<Relation> relations = new ArrayList();
         Iterator var4 = intersect.getRelations().iterator();

         while(var4.hasNext()) {
            Relation relation = (Relation)var4.next();
            relations.add((Relation)this.visitRelation(relation, context));
         }

         return (Node)intersect.getLocation().map((nodeLocation) -> {
            return new Intersect(nodeLocation, relations, intersect.isDistinct());
         }).orElse(new Intersect(relations, intersect.isDistinct()));
      }

      protected Node visitAliasedRelation(AliasedRelation aliasedRelation, DBSAstVisitorScope context) {
         DBSAstVisitorScope startOfAlias = new DBSAstVisitorScope(aliasedRelation, Optional.ofNullable(context));
         Relation relation = (Relation)this.visitRelation(aliasedRelation.getRelation(), startOfAlias);
         return (Node)aliasedRelation.getLocation().map((nodeLocation) -> {
            return new AliasedRelation(nodeLocation, relation, aliasedRelation.getAlias(), aliasedRelation.getColumnNames());
         }).orElse(new AliasedRelation(relation, aliasedRelation.getAlias(), aliasedRelation.getColumnNames()));
      }

      protected Node visitExcept(Except except, DBSAstVisitorScope context) {
         Relation relationLeft = (Relation)this.visitRelation(except.getLeft(), context);
         Relation relationRight = (Relation)this.visitRelation(except.getRight(), context);
         return (Node)except.getLocation().map((nodeLocation) -> {
            return new Except(nodeLocation, relationLeft, relationRight, except.isDistinct());
         }).orElse(new Except(relationLeft, relationRight, except.isDistinct()));
      }

      protected Node visitJoin(Join join, DBSAstVisitorScope context) {
         Relation relationLeft = (Relation)this.visitRelation(join.getLeft(), context);
         Relation relationRight = (Relation)this.visitRelation(join.getRight(), context);
         return (Node)join.getLocation().map((nodeLocation) -> {
            return new Join(nodeLocation, join.getType(), relationLeft, relationRight, join.getCriteria());
         }).orElse(new Join(join.getType(), relationLeft, relationRight, join.getCriteria()));
      }

      protected Node visitSampledRelation(SampledRelation sampledRelation, DBSAstVisitorScope context) {
         Relation relation = (Relation)this.visitRelation(sampledRelation.getRelation(), context);
         return (Node)sampledRelation.getLocation().map((nodeLocation) -> {
            return new SampledRelation(nodeLocation, relation, sampledRelation.getType(), sampledRelation.getSamplePercentage());
         }).orElse(new SampledRelation(relation, sampledRelation.getType(), sampledRelation.getSamplePercentage()));
      }

      protected Node visitSetOperation(SetOperation setOperation, DBSAstVisitorScope context) {
         return this.visitNode(setOperation, context);
      }

      protected Node visitTable(Table table, DBSAstVisitorScope context) {
         DBSRowFilterColumnMaskRewrite.log.debug("Table visited: {}", table.getName());
         String tableName = table.getName().toString();
         if (!tableName.contains(".")) {
            Optional<String> viewName = this.rootContext.getViewName(tableName);
            if (viewName.isPresent()) {
               DBSRowFilterColumnMaskRewrite.log.debug("Cte view exists for {} ", table.getName());
               return table;
            }
         }

         QualifiedObjectName qualifiedObjectName = createQualifiedObjectName(this.session, table, table.getName());
         Query query = null;
         String inlineView = this.DBSTableKeeper.getPlaceholder(qualifiedObjectName.asCatalogSchemaTableName());
         if (inlineView != null && !inlineView.isEmpty()) {
            Statement stmt = this.sqlParser.createStatement(inlineView, this.parsingOptions);
            query = (Query)stmt;
            Identifier alias = new Identifier(qualifiedObjectName.getObjectName());
            if (context != null && context.getParentNode() instanceof AliasedRelation) {
               AliasedRelation aliasedRelation = (AliasedRelation)context.getParentNode();
               if (aliasedRelation.getRelation() instanceof Table) {
                  return new TableSubquery(query);
               }
            }

            DBSRowFilterColumnMaskRewrite.log.debug("Aliased table relation being created for {}", table.getName());
            return new AliasedRelation(new TableSubquery(query), alias, (List)null);
         } else {
            DBSRowFilterColumnMaskRewrite.log.debug("Table being returned {}", table.getName());
            return table;
         }
      }

      protected Node visitTableSubquery(TableSubquery subquery, DBSAstVisitorScope context) {
         Query query = (Query)this.visitQuery(subquery.getQuery(), context);
         return (Node)subquery.getLocation().map((nodeLocation) -> {
            return new TableSubquery(nodeLocation, query);
         }).orElse(new TableSubquery(query));
      }

      protected Node visitUnion(Union subquery, DBSAstVisitorScope context) {
         List<Relation> relations = new ArrayList();
         Iterator var4 = subquery.getRelations().iterator();

         while(var4.hasNext()) {
            Relation relation = (Relation)var4.next();
            relations.add((Relation)this.visitRelation(relation, context));
         }

         return (Node)subquery.getLocation().map((nodeLocation) -> {
            return new Union(nodeLocation, relations, subquery.isDistinct());
         }).orElse(new Union(relations, subquery.isDistinct()));
      }

      protected Node visitValues(Values subquery, DBSAstVisitorScope context) {
         return this.visitNode(subquery, context);
      }

      protected Node visitUnnest(Unnest subquery, DBSAstVisitorScope context) {
         return this.visitNode(subquery, context);
      }

      protected Node visitQuery(Query node, DBSAstVisitorScope context) {
         Optional<With> with = node.getWith();
         Optional<With> visitedWith = with.map((withNode) -> {
            return (With)this.visitWith(withNode, context);
         });
         return (Node)node.getLocation().map((nodeLocation) -> {
            return new Query(nodeLocation, visitedWith, (QueryBody)this.visitQueryBody(node.getQueryBody(), context), node.getOrderBy(), node.getLimit());
         }).orElse(new Query(visitedWith, (QueryBody)this.visitQueryBody(node.getQueryBody(), context), node.getOrderBy(), node.getLimit()));
      }

      protected Node visitInsert(Insert insert, DBSAstVisitorScope context) {
         QualifiedObjectName targetTable = createQualifiedObjectName(this.session, insert, insert.getTarget());
         this.accessControl.checkCanInsertIntoTable(this.session.getIdentity(), targetTable.asCatalogSchemaTableName());
         return new Insert(insert.getTarget(), insert.getColumns(), (Query)this.visitQuery(insert.getQuery(), context));
      }

      protected Node visitQuerySpecification(QuerySpecification node, DBSAstVisitorScope context) {
         Select select = (Select)this.visitSelect(node.getSelect(), context);
         Optional<Relation> relation = node.getFrom().map((x) -> {
            return (Relation)this.visitRelation(x, context);
         });
         Optional<Expression> expression = node.getWhere().map((x) -> {
            return (Expression)this.visitExpression(x, context);
         });
         Optional<Expression> havingExpression = node.getHaving().map((x) -> {
            return (Expression)this.visitExpression(x, context);
         });
         return (Node)node.getLocation().map((nodeLocation) -> {
            return new QuerySpecification(nodeLocation, select, relation, expression, node.getGroupBy(), havingExpression, node.getOrderBy(), node.getLimit());
         }).orElse(new QuerySpecification(select, relation, expression, node.getGroupBy(), havingExpression, node.getOrderBy(), node.getLimit()));
      }
   }
}
