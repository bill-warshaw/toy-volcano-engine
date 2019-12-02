package volcano.operator;

import volcano.db.Row;
import volcano.operator.util.OutputSchema;

public interface Operator {

  /**
   * Initialize state for the operator
   */
  void open();

  /**
   * @return next input row, or null if no more input
   */
  Row next();

  /**
   * Cleanup state for the operator
   */
  void close();

  OutputSchema getOutputSchema();

  String printOperator(int indentation);
}
