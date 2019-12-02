package volcano.operator.util;

import static java.util.stream.Collectors.toList;

import java.util.List;

import volcano.db.Type;

public class OutputSchema {

  // name / alias?

  private final List<String> columnNames;
  private final List<Type> columnTypes;

  public OutputSchema(List<String> columnNames, List<Type> columnTypes) {
    this.columnNames = columnNames;
    this.columnTypes = columnTypes;
  }

  public OutputSchema(List<Column> columns) {
    this(columns.stream().map(Column::getName).collect(toList()),
        columns.stream().map(Column::getType).collect(toList()));
  }

  public List<String> getColumnNames() {
    return columnNames;
  }

  public List<Type> getColumnTypes() {
    return columnTypes;
  }

  public int columnIndex(String columnName) {
    int idx = columnNames.indexOf(columnName);
    if (idx < 0) {
      throw new IllegalArgumentException(String.format("%s not present in column names", columnName));
    }
    return idx;
  }

  public Type columnType(String columnName) {
    return columnTypes.get(columnIndex(columnName));
  }
}
