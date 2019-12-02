package volcano.operator;

import com.google.common.base.Strings;

import volcano.db.Row;
import volcano.operator.filter.FilterClause;
import volcano.operator.util.OutputSchema;

public class FilterOperator implements Operator {

  private final Operator input;
  private final FilterClause filter;

  public FilterOperator(Operator input, FilterClause filter) {
    this.input = input;
    this.filter = filter;
  }

  @Override
  public void open() {
    input.open();
  }

  @Override
  public Row next() {
    while (true) {
      Row r = input.next();
      if (r == null) {
        input.close();
        return null;
      }
      if (filter.accepts(r)) {
        return r;
      }
    }
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
    sb.append("filter[");
    sb.append(filter);
    sb.append(",");
    sb.append("input:").append("\n");
    sb.append(input.printOperator(indentation + 2));
    sb.append("]");
    return sb.toString();
  }
}
