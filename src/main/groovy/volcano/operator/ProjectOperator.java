package volcano.operator;

import static java.util.stream.Collectors.toList;

import java.util.List;

import com.google.common.base.Strings;

import volcano.db.Row;
import volcano.operator.util.Column;
import volcano.operator.util.OutputSchema;

public class ProjectOperator implements Operator {

  private final Operator input;
  private final List<Column> columns;
  private final List<Integer> columnIndexes;
  private final OutputSchema outputSchema;

  public ProjectOperator(Operator input, List<Column> columns) {
    this.input = input;
    this.columns = columns;
    this.columnIndexes = columns.stream()
        .map(c -> input.getOutputSchema().columnIndex(c.getName()))
        .collect(toList());
    this.outputSchema = new OutputSchema(columns);
  }

  @Override
  public void open() {
    input.open();
  }

  @Override
  public Row next() {
    Row r = input.next();
    if (r == null) {
      input.close();
      return null;
    }
    return new Row(columnIndexes.stream().map(r::getAt).collect(toList()));
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
    StringBuilder sb = new StringBuilder();
    sb.append(Strings.repeat(" ", indentation));
    sb.append("project[");
    sb.append("columns:");
    sb.append(columns);
    sb.append(",");
    sb.append("input:").append("\n");
    sb.append(input.printOperator(indentation + 2));
    sb.append("\n");
    sb.append(Strings.repeat(" ", indentation));
    sb.append("]");
    return sb.toString();
  }
}
