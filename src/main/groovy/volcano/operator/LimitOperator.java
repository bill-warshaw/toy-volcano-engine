package volcano.operator;

import com.google.common.base.Strings;

import volcano.db.Row;
import volcano.operator.util.OutputSchema;

public class LimitOperator implements Operator {

  private final Operator input;
  private final int limit;
  private int numRowsEmitted;

  public LimitOperator(Operator input, int limit) {
    this.input = input;
    this.limit = limit;
    this.numRowsEmitted = 0;
  }

  @Override
  public void open() {
    input.open();
  }

  @Override
  public Row next() {
    if (numRowsEmitted >= limit) {
      input.close();
      return null;
    }
    numRowsEmitted++;
    // if limit not reached but no more input, will return null
    return input.next();
  }

  @Override
  public void close() {
  }

  @Override
  public OutputSchema getOutputSchema() {
    return input.getOutputSchema();
  }

  @Override
  public String printOperator(int indentation) {
    StringBuilder sb = new StringBuilder();
    sb.append(Strings.repeat(" ", indentation));
    sb.append("limit[limit:");
    sb.append(limit);
    sb.append(",");
    sb.append("input:").append("\n");
    sb.append(input.printOperator(indentation + 2));
    sb.append("]");
    return sb.toString();
  }
}
