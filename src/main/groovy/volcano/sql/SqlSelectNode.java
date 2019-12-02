package volcano.sql;

import static java.util.stream.Collectors.toList;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import volcano.db.Database;
import volcano.operator.AggregateOperator;
import volcano.operator.DistinctOperator;
import volcano.operator.FilterOperator;
import volcano.operator.LimitOperator;
import volcano.operator.Operator;
import volcano.operator.ProjectOperator;
import volcano.operator.ScanOperator;
import volcano.operator.SortOperator;
import volcano.operator.filter.FilterLogicalOp;
import volcano.operator.sort.SortOrder;

public class SqlSelectNode implements SqlNode {

  private final List<String> columnsToProject;
  private final String tableName;
  private final SqlFilterNode filterNode;
  private final Map<String,List<String>> sorts;
  private final Database db;
  private final Integer limit;
  private final boolean distinct;
  private final List<String> groupingColumns;
  private final List<String> aggrFns;

  public SqlSelectNode(Map<String,Object> jsonNode, Database db) {
    this.db = db;
    aggrFns = new ArrayList<>();
    groupingColumns = parseGroupingColumns(jsonNode);
    columnsToProject = parseColumns(jsonNode);
    tableName = parseTableName(jsonNode);
    filterNode = parseFilterNode(jsonNode);
    sorts = parseSorts(jsonNode);
    limit = parseLimit(jsonNode);
    distinct = parseDistinct(jsonNode);
  }
  //todo
  // having?

  //todo these could have fns too
  private List<String> parseColumns(Map<String,Object> jsonNode) {
    Object columnsJson = jsonNode.get("columns");
    if (columnsJson.equals("*")) {
      // * - all columns - will treat empty array as wildcard
      // if non-empty, add project operator at root of tree
      return new ArrayList<>();
    } else {
      List<Map<String,Object>> ccs = (List<Map<String,Object>>)columnsJson;
      List<String> colDefs = new ArrayList<>();
      for (Map<String,Object> c : ccs) {
        if (groupingColumns.isEmpty()) {
          colDefs.add((String)((Map<String,Object>)c.get("expr")).get("column"));
        } else {
          Map<String,Object> cExpr = (Map<String,Object>)c.get("expr");
          if (cExpr.get("type").equals("aggr_func")) {
            String aggrFn = (String)cExpr.get("name");
            Map<String,Object> aggrArgs = (Map<String,Object>)cExpr.get("args");
            Map<String,Object> aggrArgsExpr = (Map<String,Object>)aggrArgs.get("expr");
            String aggCol = (String)aggrArgsExpr.get("column");
            colDefs.add(aggCol);
            aggrFns.add(aggrFn);
          }
        }
      }
      return colDefs;
    }
  }

  //todo multiple tables for FROM for joins
  private String parseTableName(Map<String,Object> jsonNode) {
    List<Map<String,Object>> from = (List<Map<String,Object>>)jsonNode.get("from");
    if (from.size() != 1) {
      throw new IllegalArgumentException(
          "We don't support joins yet, but query is selecting from multiple tables");
    }
    return (String)from.get(0).get("table");
  }

  private SqlFilterNode parseFilterNode(Map<String,Object> jsonNode) {
    if (jsonNode.get("where") == null) {
      return null;
    } else {
      Map<String,Object> where = (Map<String,Object>)jsonNode.get("where");
      if (where.get("type").equals("column_ref")) {
        // treat 'where col' as 'where col = true'
        return new SqlFilterNode((String)where.get("column"), FilterLogicalOp.EQ, true, tableName);
      } else if (where.get("type").equals("unary_expr")) {
        // treat 'where not col' as 'where col <> true'
        if (!where.get("operator").equals("NOT")) {
          throw new IllegalArgumentException(
              String.format("NOT is the only supported unary operator; received %s", where.get("operator")));
        }
        Map<String,Object> expr = (Map<String,Object>)where.get("expr");
        String filterColumn = (String)expr.get("column");
        return new SqlFilterNode(filterColumn, FilterLogicalOp.NEQ, true, tableName);
      } else if (where.get("type").equals("binary_expr")) {
        FilterLogicalOp op = FilterLogicalOp.findBySqlOp((String)where.get("operator"));
        Map<String,Object> left = (Map<String,Object>)where.get("left");
        if (!left.get("type").equals("column_ref")) {
          throw new IllegalArgumentException(
              String.format("Only column refs are supported as left value of filter; received %s", left));
        }
        String filterColumn = (String)left.get("column");

        Map<String,Object> right = (Map<String,Object>)where.get("right");
        if (!right.containsKey("value")) {
          throw new IllegalArgumentException(
              String.format("Only values are supported as right value of filter; received %s", right));
        }
        Object filterValue = right.get("value");
        return new SqlFilterNode(filterColumn, op,
            (db.getTable(tableName).fieldType(db.getTable(tableName).fieldIdx(filterColumn))).cast(
                filterValue), tableName);
      } else {
        throw new IllegalArgumentException(
            String.format("Unrecognized where clause type: %s", where.get("type")));
      }
    }
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

  private List<String> parseGroupingColumns(Map<String,Object> jsonNode) {
    if (jsonNode.get("groupby") == null) {
      return new ArrayList<>();
    }
    List<Map<String,Object>> groupBy = (List<Map<String,Object>>)jsonNode.get("groupby");
    //todo - handle non column_refs, and other tables
    List<String> gCols = new ArrayList<>();
    groupBy.forEach(g -> {
      if (!g.get("type").equals("column_ref")) {
        throw new IllegalArgumentException(String.format("Only support grouping on columns; received %s", g));
      }
      gCols.add((String)g.get("column"));
    });
    return gCols;
  }

  @Override
  public Operator toOperator(Database db) {
    Operator rootOperator = new ScanOperator(db, tableName);
    if (filterNode != null) {
      rootOperator = new FilterOperator(rootOperator, filterNode.getFilter(db));
    }
    if (!groupingColumns.isEmpty()) {
      rootOperator = new AggregateOperator(rootOperator, groupingColumns, columnsToProject, aggrFns);
    }
    if (!sorts.isEmpty()) {
      rootOperator = new SortOperator(rootOperator, sorts.get("columns"),
          sorts.get("orders").stream().map(SortOrder::valueOf).collect(toList()));
    }
    if (distinct) {
      rootOperator = new DistinctOperator(rootOperator, columnsToProject);
    }
    if (!columnsToProject.isEmpty()) {
      if (!groupingColumns.isEmpty()) {
        List<String> cols = new ArrayList<>();
        cols.addAll(groupingColumns);
        cols.addAll(columnsToProject);
        rootOperator = new ProjectOperator(rootOperator, cols);
      } else {
        rootOperator = new ProjectOperator(rootOperator, columnsToProject);
      }
    }
    if (limit != null) {
      rootOperator = new LimitOperator(rootOperator, limit);
    }
    return rootOperator;
  }
}
