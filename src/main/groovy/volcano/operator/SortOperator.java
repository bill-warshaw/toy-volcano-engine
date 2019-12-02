package volcano.operator;

import static java.util.stream.Collectors.toList;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import com.google.common.base.Strings;

import volcano.db.Row;
import volcano.operator.sort.SortOrder;
import volcano.operator.util.OutputSchema;

public class SortOperator implements Operator {

  private final Operator input;
  private final List<String> columns;
  private final List<SortOrder> sortOrders;
  private final List<Integer> columnIndexes;

  private final List<Row> rows;
  private int nextIndex;

  public SortOperator(Operator input, List<String> columns, List<SortOrder> sortOrders) {
    this.input = input;
    this.columns = columns;
    this.sortOrders = sortOrders;
    if (columns.size() != sortOrders.size()) {
      throw new IllegalArgumentException(
          String.format("column name [%d] and sort order [%d] lists are mismatched", columns.size(),
              sortOrders.size()));
    }
    if (columns.isEmpty()) {
      throw new IllegalArgumentException("No ordering specified for orderBy node");
    }
    this.columnIndexes = columns.stream().map(c -> input.getOutputSchema().columnIndex(c)).collect(toList());
    this.rows = new ArrayList<>();
    this.nextIndex = 0;
  }

  // this needs to block until sub operators return entire result set
  @Override
  public void open() {
    input.open();
    while (true) {
      Row r = input.next();
      if (r == null) {
        break;
      }
      rows.add(r);
    }
    // apply sorts in reverse order
    for (int i = columnIndexes.size() - 1; i >= 0; i--) {
      int colIdx = columnIndexes.get(i);
      if (sortOrders.get(i) == SortOrder.ASC) {
        rows.sort(Comparator.comparing(a -> a.getAt(colIdx)));
      } else {
        rows.sort((a, b) -> b.getAt(colIdx).compareTo(a.getAt(colIdx)));
      }
    }
  }

  @Override
  public Row next() {
    if (nextIndex == rows.size()) {
      input.close();
      return null;
    }
    Row r = rows.get(nextIndex);
    nextIndex++;
    return r;
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
    sb.append("sort[");
    sb.append("columns:");
    for (int i = 0; i < columns.size(); i++) {
      sb.append(columns.get(i)).append(" ").append(sortOrders.get(i));
      if (i != columns.size() - 1) {
        sb.append(",");
      }
    }
    sb.append(",");
    sb.append("input:").append("\n");
    sb.append(input.printOperator(indentation + 2));
    sb.append("]");
    return sb.toString();
  }
}
