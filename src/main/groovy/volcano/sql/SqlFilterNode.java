package volcano.sql;

import volcano.db.Database;
import volcano.operator.Operator;
import volcano.operator.filter.FilterClause;
import volcano.operator.filter.FilterLogicalOp;

public class SqlFilterNode implements SqlNode {

  private final String column;
  private final FilterLogicalOp op;
  private final Comparable value;
  private final String table;

  SqlFilterNode(String column, FilterLogicalOp op, Comparable value, String table) {
    this.column = column;
    this.op = op;
    this.value = value;
    this.table = table;
  }

  @Override
  public Operator toOperator(Database db) {
    return null;
  }

  public FilterClause getFilter(Database db) {
    return new FilterClause(db.getTable(table).fieldIdx(column), op, value);
  }
}
