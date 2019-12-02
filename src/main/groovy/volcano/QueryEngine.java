package volcano;

import java.util.ArrayList;
import java.util.List;

import volcano.db.Database;
import volcano.db.Row;
import volcano.operator.Operator;
import volcano.sql.SqlAstParser;

public class QueryEngine {

  private final Database db;
  private final SqlAstParser sqlAstParser;

  public QueryEngine(Database db, String sqlAstParserHostname) {
    this.db = db;
    this.sqlAstParser = new SqlAstParser(sqlAstParserHostname, 7001);
  }

  List<Row> executeQuery(String sqlStmt) throws Exception {
    Operator operatorTree = sqlAstParser.parse(sqlStmt, db);
    System.out.println(operatorTree.printOperator(0));
    operatorTree.open();
    List<Row> rows = new ArrayList<>();
    while (true) {
      Row row = operatorTree.next();
      if (row == null) {
        break;
      }
      rows.add(row);
    }
    operatorTree.close();
    return rows;
  }
}
