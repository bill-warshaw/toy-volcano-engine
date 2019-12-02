package volcano.operator;

import static java.util.stream.Collectors.toList;

import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import com.google.common.base.Strings;

import volcano.db.Row;
import volcano.operator.util.OutputSchema;

public class DistinctOperator implements Operator {

  private final Operator input;
  private final Set<Integer> previouslySeenRowHashes;
  private final List<Integer> columnIndexes;

  // distinctColumns maps to projected columns for this select node
  public DistinctOperator(Operator input, List<String> distinctColumns) {
    this.input = input;
    this.previouslySeenRowHashes = new HashSet<>();
    this.columnIndexes = distinctColumns.stream()
        .map(c -> input.getOutputSchema().columnIndex(c))
        .collect(toList());
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
      Object[] distinctCandidateElements = columnIndexes.stream().map(r::getAt).toArray();
      int rowHash = Objects.hash(distinctCandidateElements);
      if (!previouslySeenRowHashes.contains(rowHash)) {
        previouslySeenRowHashes.add(rowHash);
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
    sb.append("distinct[");
    sb.append("input:").append("\n");
    sb.append(input.printOperator(indentation + 2));
    sb.append("]");
    return sb.toString();
  }
}
