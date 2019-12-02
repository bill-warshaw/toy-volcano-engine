package volcano.db;

import java.util.Map;

public class Database {

  private final Map<String,Table> tables;

  public Database(Map<String,Table> tables) {
    this.tables = tables;
  }

  public Table getTable(String tableName) {
    if (!tables.containsKey(tableName)) {
      throw new IllegalArgumentException(String.format("Table %s does not exist", tableName));
    }
    return tables.get(tableName);
  }
}
