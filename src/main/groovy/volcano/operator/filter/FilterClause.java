package volcano.operator.filter;

import static volcano.operator.filter.FilterLogicalOp.BETWEEN;
import static volcano.operator.filter.FilterLogicalOp.EQ;
import static volcano.operator.filter.FilterLogicalOp.GT;
import static volcano.operator.filter.FilterLogicalOp.GTE;
import static volcano.operator.filter.FilterLogicalOp.IN;
import static volcano.operator.filter.FilterLogicalOp.LIKE;
import static volcano.operator.filter.FilterLogicalOp.LT;
import static volcano.operator.filter.FilterLogicalOp.LTE;
import static volcano.operator.filter.FilterLogicalOp.NEQ;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Pattern;

import volcano.db.Row;

//todo can filters use multiple fields?
// e.g select * from tbl where (a+b) > 5
public class FilterClause {
  private static final Set<FilterLogicalOp> scalarOps = new HashSet<>(
      Arrays.asList(EQ, NEQ, GT, GTE, LT, LTE, LIKE));

  private final String column;
  private final FilterLogicalOp op;
  private final Comparable value;
  private Optional<Pattern> regex;
  private Optional<List<Comparable>> betweenClause;
  private Optional<List<Comparable>> inClause;

  public FilterClause(String column, FilterLogicalOp op, Comparable value) {
    this.column = column;
    this.op = op;
    this.value = value;
    if (scalarOps.contains(op)) {
      if (value instanceof List) {
        throw new IllegalArgumentException(
            String.format("Expected scalar argument for %s; received %s", op, value));
      }
    } else if (op == BETWEEN) {
      if (!(value instanceof List)) {
        throw new IllegalArgumentException(String.format("Expected list for BETWEEN; received %s", value));
      }
      List l = (List)value;
      if (l.size() != 2) {
        throw new IllegalArgumentException(
            String.format("Expected 2 item list for BETWEEN; received %s", value));
      }
      betweenClause = Optional.of(l);
    } else if (op == IN) {
      if (!(value instanceof List)) {
        throw new IllegalArgumentException(String.format("Expected list for IN; received %s", value));
      }
      inClause = Optional.of((List)value);
    }

    if (op == LIKE) {
      if (!(value instanceof String)) {
        throw new IllegalArgumentException(String.format("Expected string for LIKE; received %s", value));
      }
      regex = Optional.of(Pattern.compile((String)value));
    }
  }

  public boolean accepts(Row row, int fieldIndex) {
    switch (op) {
    case EQ:
      return row.getAt(fieldIndex).equals(value);
    case NEQ:
      return !row.getAt(fieldIndex).equals(value);
    case GT:
      return row.getAt(fieldIndex).compareTo(value) > 0;
    case GTE:
      return row.getAt(fieldIndex).compareTo(value) >= 0;
    case LT:
      return row.getAt(fieldIndex).compareTo(value) < 0;
    case LTE:
      return row.getAt(fieldIndex).compareTo(value) <= 0;
    case LIKE:
      return regex.get().matcher((String)value).matches();
    case BETWEEN:
      Comparable v = row.getAt(fieldIndex);
      Comparable lowerBound = betweenClause.get().get(0);
      Comparable upperBound = betweenClause.get().get(1);
      return v.compareTo(lowerBound) >= 0 && v.compareTo(upperBound) <= 0;
    case IN:
      return inClause.get().contains(row.getAt(fieldIndex));
    default:
      throw new IllegalArgumentException(String.format("Unrecognized filter operator [%s]", op));
    }
  }

  public String getColumn() {
    return column;
  }

  @Override
  public String toString() {
    return String.format("column: %s, op: %s, value: %s", column, op, value);
  }
}
