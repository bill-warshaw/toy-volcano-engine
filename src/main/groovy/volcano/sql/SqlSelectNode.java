package volcano.sql;

import static java.util.stream.Collectors.toList;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import com.google.common.collect.ImmutableList;

import volcano.db.Database;
import volcano.operator.AggregateOperator;
import volcano.operator.DistinctOperator;
import volcano.operator.FilterOperator;
import volcano.operator.HashJoinOperator;
import volcano.operator.LimitOperator;
import volcano.operator.Operator;
import volcano.operator.ProjectOperator;
import volcano.operator.ScanOperator;
import volcano.operator.SortOperator;
import volcano.operator.aggregate.AggregateFn;
import volcano.operator.sort.SortOrder;
import volcano.operator.util.Column;

public class SqlSelectNode implements SqlNode {

  private final SqlFilterNode filterNode;
  private final Map<String,List<String>> sorts;
  private final Database db;
  private final Integer limit;
  private final boolean distinct;
  private final List<String> tableNames;

  private final List<Column> columns;

  private final List<List<String>> joinColumns;

  //todo reorder result set columns?
  public SqlSelectNode(Map<String,Object> jsonNode, Database db) {
    this.db = db;
    columns = new ArrayList<>();
    joinColumns = new ArrayList<>();
    tableNames = parseTableNames(jsonNode);
    parseGroupingColumns(jsonNode);
    parseColumns(jsonNode);
    filterNode = SqlFilterNode.parseFilterNode(jsonNode, db, tableNames);
    sorts = parseSorts(jsonNode);
    limit = parseLimit(jsonNode);
    distinct = parseDistinct(jsonNode);
  }

  //todo these could have fns too
  //todo handle aliases
  private void parseColumns(Map<String,Object> jsonNode) {
    Object columnsJson = jsonNode.get("columns");
    if (columnsJson.equals("*")) {
      throw new IllegalArgumentException("Projection columns must be explicitly specified, but received *");
    }
    List<Map<String,Object>> ccs = (List<Map<String,Object>>)columnsJson;
    for (Map<String,Object> c : ccs) {
      Map<String,Object> exprNode = (Map<String,Object>)c.get("expr");
      if (exprNode.get("type").equals("column_ref")) {
        String table = (String)exprNode.get("table");
        if (table == null) {
          throw new IllegalArgumentException(
              String.format("Must specify table for projected columns %s", exprNode));
        }
        String name = (String)exprNode.get("column");
        if (columns.stream().noneMatch(t -> t.getName().equals(name))) {
          // grouping column is also specified in projections in AST
          columns.add(new Column(name, db.getTable(table).fieldType(name), Optional.empty(), false));
        }
      } else if (exprNode.get("type").equals("aggr_func")) {
        //todo this needs to be a more generic traversal
        String aggrFn = (String)exprNode.get("name");
        Map<String,Object> aggrArgs = (Map<String,Object>)exprNode.get("args");
        Map<String,Object> aggrArgsExpr = (Map<String,Object>)aggrArgs.get("expr");
        String name = (String)aggrArgsExpr.get("column");
        String table = (String)aggrArgsExpr.get("table");
        columns.add(
            new Column(name, db.getTable(table).fieldType(name), Optional.of(AggregateFn.valueOf(aggrFn)),
                false));
      } else {
        throw new IllegalArgumentException(String.format("Unrecognized column type %s", exprNode));
      }
    }
  }

  //todo if not using `join on` syntax, join filters can appear in where clause
  private List<String> parseTableNames(Map<String,Object> jsonNode) {
    List<Map<String,Object>> from = (List<Map<String,Object>>)jsonNode.get("from");
    if (from.size() > 1) {
      if (from.size() > 2) {
        throw new IllegalArgumentException("Only support two-way joins");
      }
      Map<String,Object> secondTable = from.get(1);
      if (!secondTable.get("join").equals("INNER JOIN")) {
        throw new IllegalArgumentException("Only support inner joins");
      }
      Map<String,Object> on = (Map<String,Object>)secondTable.get("on");
      if (on.get("type").equals("column_ref")) {
        String joinCol = (String)on.get("column");
        joinColumns.add(new ArrayList<>(ImmutableList.of(joinCol)));
        joinColumns.add(new ArrayList<>(ImmutableList.of(joinCol)));
      } else if (on.get("type").equals("binary_expr")) {
        if (!on.get("operator").equals("=")) {
          throw new IllegalArgumentException("Only support equijoins");
        }
        Map<String,Object> left = (Map<String,Object>)on.get("left");
        Map<String,Object> right = (Map<String,Object>)on.get("right");
        if (!left.get("type").equals("column_ref") || !right.get("type").equals("column_ref")) {
          throw new IllegalArgumentException("Don't support joining on subqueries");
        }
        joinColumns.add(new ArrayList<>(ImmutableList.of((String)left.get("column"))));
        joinColumns.add(new ArrayList<>(ImmutableList.of((String)right.get("column"))));
      } else {
        throw new IllegalArgumentException("Don't support joining on subqueries");
      }
    }
    return from.stream().map(m -> (String)m.get("table")).collect(toList());
  }

  private Map<String,List<String>> parseSorts(Map<String,Object> jsonNode) {
    List<Map<String,Object>> sortNode = (List<Map<String,Object>>)jsonNode.get("orderby");
    if (sortNode == null) {
      return new HashMap<>();
    }
    Map<String,List<String>> sorts = new HashMap<>();
    List<String> sortCols = new ArrayList<>();
    List<String> sortOrders = new ArrayList<>();

    for (Map<String,Object> sort : sortNode) {
      String type = (String)sort.get("type");
      Map<String,Object> expr = (Map<String,Object>)sort.get("expr");
      if (!expr.get("type").equals("column_ref")) {
        throw new IllegalArgumentException(
            String.format("Only support sorts against columns; received %s", expr));
      }
      String column = (String)expr.get("column");
      sortCols.add(column);
      sortOrders.add(type);
    }

    sorts.put("columns", sortCols);
    sorts.put("orders", sortOrders);
    return sorts;
  }

  private Integer parseLimit(Map<String,Object> jsonNode) {
    //[ { type: 'number', value: 0 }, { type: 'number', value: 5 } ] }
    if (jsonNode.get("limit") == null) {
      return null;
    }
    List<Map<String,Object>> limit = (List<Map<String,Object>>)jsonNode.get("limit");
    // dummy limit passed at index 0
    if (limit.size() != 2) {
      throw new IllegalArgumentException(String.format("Multiple limits not supported - %s", limit));
    }
    // comes back as a double from JSON SQL service
    return ((Double)limit.get(1).get("value")).intValue();
  }

  private boolean parseDistinct(Map<String,Object> jsonNode) {
    return "DISTINCT".equals(jsonNode.get("distinct"));
  }

  private void parseGroupingColumns(Map<String,Object> jsonNode) {
    if (jsonNode.get("groupby") == null) {
      return;
    }
    List<Map<String,Object>> groupBy = (List<Map<String,Object>>)jsonNode.get("groupby");
    //todo - handle non column_refs, and other tables
    List<Column> gCols = new ArrayList<>();
    groupBy.forEach(g -> {
      if (!g.get("type").equals("column_ref")) {
        throw new IllegalArgumentException(String.format("Only support grouping on columns; received %s", g));
      }
      String name = (String)g.get("column");
      String table = (String)g.get("table");
      gCols.add(
          new Column(name, db.getTable(table).fieldType(db.getTable(table).fieldIdx(name)), Optional.empty(),
              true));
    });
    columns.addAll(gCols);
  }

  @Override
  public Operator toOperator(Database db) {
    Operator rootOperator;
    if (tableNames.size() > 1) {
      Operator leftInput = new ScanOperator(db, tableNames.get(0));
      Operator rightInput = new ScanOperator(db, tableNames.get(1));
      rootOperator = new HashJoinOperator(leftInput, rightInput, joinColumns.get(0), joinColumns.get(1));
    } else {
      rootOperator = new ScanOperator(db, tableNames.get(0));
    }
    if (filterNode != null) {
      rootOperator = new FilterOperator(rootOperator, filterNode.getFilter());
    }
    if (columns.stream().anyMatch(c -> c.getFn().isPresent())) {
      rootOperator = new AggregateOperator(rootOperator, columns);
    }
    if (!sorts.isEmpty()) {
      rootOperator = new SortOperator(rootOperator, sorts.get("columns"),
          sorts.get("orders").stream().map(SortOrder::valueOf).collect(toList()));
    }
    if (distinct) {
      rootOperator = new DistinctOperator(rootOperator, columns);
    }
    rootOperator = new ProjectOperator(rootOperator, columns);
    if (limit != null) {
      rootOperator = new LimitOperator(rootOperator, limit);
    }
    return rootOperator;
  }
}
