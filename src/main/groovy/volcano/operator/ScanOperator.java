package volcano.operator;

import java.util.Iterator;

import com.google.common.base.Strings;

import volcano.db.Database;
import volcano.db.Row;
import volcano.db.Table;
import volcano.operator.util.OutputSchema;

public class ScanOperator implements Operator {

  private final Database db;
  private final String tableName;
  private final OutputSchema outputSchema;
  private Iterator<Row> rows;

  public ScanOperator(Database db, String tableName) {
    this.db = db;
    this.tableName = tableName;
    Table t = db.getTable(tableName);
    this.outputSchema = new OutputSchema(t.getColumnNames(), t.getColumnTypes());
  }

  @Override
  public void open() {
    Table t = db.getTable(tableName);
    this.rows = t.rows();
  }

  @Override
  public Row next() {
    if (!rows.hasNext()) {
      return null;
    }
    return rows.next();
  }

  @Override
  public void close() {
  }

  @Override
  public OutputSchema getOutputSchema() {
    return outputSchema;
  }

  @Override
  public String printOperator(int indentation) {
    return Strings.repeat(" ", indentation) + "select[" + tableName + "]";
  }
}
