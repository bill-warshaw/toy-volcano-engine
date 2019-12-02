package volcano.sql;

import java.util.List;
import java.util.Map;

import volcano.db.Database;
import volcano.operator.Operator;
import volcano.operator.filter.FilterClause;
import volcano.operator.filter.FilterLogicalOp;

public class SqlFilterNode implements SqlNode {

  private final String column;
  private final FilterLogicalOp op;
  private final Comparable value;

  private SqlFilterNode(String column, FilterLogicalOp op, Comparable value) {
    this.column = column;
    this.op = op;
    this.value = value;
  }

  static SqlFilterNode parseFilterNode(Map<String,Object> jsonNode, Database db, List<String> tableNames) {
    if (jsonNode.get("where") == null) {
      return null;
    } else {
      Map<String,Object> where = (Map<String,Object>)jsonNode.get("where");
      if (where.get("type").equals("column_ref")) {
        // treat 'where col' as 'where col = true'
        return new SqlFilterNode((String)where.get("column"), FilterLogicalOp.EQ, true);
      } else if (where.get("type").equals("unary_expr")) {
        // treat 'where not col' as 'where col <> true'
        if (!where.get("operator").equals("NOT")) {
          throw new IllegalArgumentException(
              String.format("NOT is the only supported unary operator; received %s", where.get("operator")));
        }
        Map<String,Object> expr = (Map<String,Object>)where.get("expr");
        String filterColumn = (String)expr.get("column");
        return new SqlFilterNode(filterColumn, FilterLogicalOp.NEQ, true);
      } else if (where.get("type").equals("binary_expr")) {
        FilterLogicalOp op = FilterLogicalOp.findBySqlOp((String)where.get("operator"));
        Map<String,Object> left = (Map<String,Object>)where.get("left");
        if (!left.get("type").equals("column_ref")) {
          throw new IllegalArgumentException(
              String.format("Only column refs are supported as left value of filter; received %s", left));
        }
        String filterColumn = (String)left.get("column");
        String tableName = (String)left.get("table");

        Map<String,Object> right = (Map<String,Object>)where.get("right");
        if (!right.containsKey("value")) {
          throw new IllegalArgumentException(
              String.format("Only values are supported as right value of filter; received %s", right));
        }
        Object filterValue = right.get("value");
        return new SqlFilterNode(filterColumn, op,
            (db.getTable(tableName).fieldType(db.getTable(tableName).fieldIdx(filterColumn))).cast(
                filterValue));
      } else {
        throw new IllegalArgumentException(
            String.format("Unrecognized where clause type: %s", where.get("type")));
      }
    }
  }

  @Override
  public Operator toOperator(Database db) {
    return null;
  }

  public FilterClause getFilter() {
    return new FilterClause(column, op, value);
  }
}
