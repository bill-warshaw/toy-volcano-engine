package volcano.db;

import java.util.Iterator;
import java.util.List;
import java.util.Objects;

public class Table {

  private final List<String> columnNames;
  private final List<Type> columnTypes;
  private final List<Row> rows;
  private final String tableName;

  public Table(String tableName, List<String> columnNames, List<Type> columnTypes, List<Row> rows) {
    this.tableName = tableName;
    if (columnNames.size() != columnTypes.size()) {
      throw new IllegalArgumentException(
          String.format("Mismatched column names [%d] and types [%d]", columnNames.size(),
              columnTypes.size()));
    }
    for (int i = 0; i < rows.size(); i++) {
      Row r = rows.get(i);
      if (r.size() != columnNames.size()) {
        throw new IllegalArgumentException(
            String.format("Row %d has incorrect number of elements, expected %d but was %d", i,
                columnNames.size(), r.size()));
      }
    }
    this.columnNames = columnNames;
    this.columnTypes = columnTypes;
    this.rows = rows;
  }

  public Iterator<Row> rows() {
    return rows.iterator();
  }

  public int fieldIdx(String columnName) {
    if (!columnNames.contains(columnName)) {
      throw new IllegalArgumentException(
          String.format("Invalid column name for %s: %s", tableName, columnName));
    }
    return columnNames.indexOf(columnName);
  }

  public Type fieldType(int fieldIndex) {
    return columnTypes.get(fieldIndex);
  }

  public List<String> getColumnNames() {
    return columnNames;
  }

  public List<Type> getColumnTypes() {
    return columnTypes;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("Table[");
    sb.append("name:");
    sb.append(tableName);
    sb.append(",");
    sb.append("columnNames:");
    sb.append(columnNames.toString());
    sb.append(",");
    sb.append("columnTypes:");
    sb.append(columnTypes.toString());
    sb.append(",");
    sb.append("rows:");
    sb.append(rows.toString());
    sb.append("]");
    return sb.toString();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o)
      return true;
    if (o == null || getClass() != o.getClass())
      return false;
    Table table = (Table)o;
    return Objects.equals(columnNames, table.columnNames) && Objects.equals(columnTypes, table.columnTypes) &&
        Objects.equals(rows, table.rows);
  }

  @Override
  public int hashCode() {
    return Objects.hash(columnNames, columnTypes, rows);
  }
}
