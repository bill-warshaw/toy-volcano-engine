package volcano.sql;

import volcano.db.Database;
import volcano.operator.Operator;

public interface SqlNode {

  Operator toOperator(Database db);
}
