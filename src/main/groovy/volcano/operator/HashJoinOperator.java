package volcano.operator;

import static java.util.stream.Collectors.toList;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import com.google.common.base.Strings;

import volcano.db.Row;
import volcano.db.Type;
import volcano.operator.util.OutputSchema;

public class HashJoinOperator implements Operator {

  private final Operator leftInput;
  private final Operator rightInput;
  private final List<String> leftJoinCols;
  private final List<String> rightJoinCols;
  private final Map<Object,List<Row>> probeTable;

  private final List<Integer> leftColJoinIndexes;
  private final List<Integer> rightColJoinIndexes;

  private List<Row> temporaryMultipleBuildRowsForKeys;
  private Row temporaryProbeRow;

  // ONLY WORKS FOR EQUI JOINS
  public HashJoinOperator(
      Operator leftInput, Operator rightInput, List<String> leftJoinCols, List<String> rightJoinCols) {
    this.leftInput = leftInput;
    this.rightInput = rightInput;
    this.leftJoinCols = leftJoinCols;
    this.rightJoinCols = rightJoinCols;
    this.probeTable = new HashMap<>();
    this.leftColJoinIndexes = leftJoinCols.stream()
        .map(c -> leftInput.getOutputSchema().columnIndex(c))
        .collect(toList());
    this.rightColJoinIndexes = rightJoinCols.stream()
        .map(c -> rightInput.getOutputSchema().columnIndex(c))
        .collect(toList());
    this.temporaryMultipleBuildRowsForKeys = new LinkedList<>();
  }

  @Override
  public void open() {
    rightInput.open();
    while (true) {
      Row r = rightInput.next();
      if (r == null) {
        rightInput.close();
        break;
      }
      List<Object> joinCols = rightColJoinIndexes.stream().map(r::getAt).collect(toList());
      if (!probeTable.containsKey(joinCols)) {
        probeTable.put(joinCols, new ArrayList<>());
      }
      probeTable.get(joinCols).add(r);
    }
    leftInput.open();
  }

  @Override
  public Row next() {
    if (!temporaryMultipleBuildRowsForKeys.isEmpty()) {
      Row buildTableRow = temporaryMultipleBuildRowsForKeys.remove(0);
      return temporaryProbeRow.combine(buildTableRow);
    }
    while (true) {
      Row r = leftInput.next();
      if (r == null) {
        leftInput.close();
        return null;
      }
      List<Object> joinCols = leftColJoinIndexes.stream().map(r::getAt).collect(toList());
      List<Row> rows = probeTable.get(joinCols);
      if (rows != null) {
        if (rows.size() > 1) {
          temporaryProbeRow = r;
          temporaryMultipleBuildRowsForKeys.addAll(rows);
          Row buildTableRow = temporaryMultipleBuildRowsForKeys.remove(0);
          return temporaryProbeRow.combine(buildTableRow);
        } else {
          return r.combine(rows.get(0));
        }
      }
    }
  }

  @Override
  public void close() {

  }

  @Override
  public OutputSchema getOutputSchema() {
    OutputSchema leftSchema = leftInput.getOutputSchema();
    OutputSchema rightSchema = rightInput.getOutputSchema();
    List<String> names = new ArrayList<>();
    names.addAll(leftSchema.getColumnNames());
    names.addAll(rightSchema.getColumnNames());
    List<Type> types = new ArrayList<>();
    types.addAll(leftSchema.getColumnTypes());
    types.addAll(rightSchema.getColumnTypes());
    return new OutputSchema(names, types);
  }

  @Override
  public String printOperator(int indentation) {
    StringBuilder sb = new StringBuilder();
    sb.append(Strings.repeat(" ", indentation));
    sb.append("join[");
    sb.append("leftColumns:");
    sb.append(leftJoinCols);
    sb.append(",");
    sb.append("rightColumns:");
    sb.append(rightJoinCols);
    sb.append(",");
    sb.append("\n");
    sb.append(Strings.repeat(" ", indentation + 2));
    sb.append("left_input:").append("\n");
    sb.append(leftInput.printOperator(indentation + 4));
    sb.append("\n");
    sb.append(Strings.repeat(" ", indentation + 2));
    sb.append("right_input:").append("\n");
    sb.append(rightInput.printOperator(indentation + 4));
    sb.append("\n");
    sb.append(Strings.repeat(" ", indentation));
    sb.append("]");
    return sb.toString();
  }
}
