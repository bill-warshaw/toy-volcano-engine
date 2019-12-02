package volcano.operator.util;

import java.util.Optional;

import volcano.db.Type;
import volcano.operator.aggregate.AggregateFn;

public final class Column {

  //todo - support complex functions

  private final String name;
  private final Type type;
  private final Optional<AggregateFn> fn;
  private final boolean grouping;

  public Column(String name, Type type, Optional<AggregateFn> fn, boolean grouping) {
    this.name = name;
    this.type = type;
    this.fn = fn;
    this.grouping = grouping;
  }

  public String getName() {
    return name;
  }

  public Type getType() {
    return type;
  }

  public Optional<AggregateFn> getFn() {
    return fn;
  }

  public boolean isGrouping() {
    return grouping;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("[");
    fn.ifPresent(f -> {
      sb.append(fn.get());
      sb.append("(");
    });
    sb.append(name);
    fn.ifPresent(f -> sb.append(")"));
    if (isGrouping()) {
      sb.append(", isGrouping: true");
    }
    sb.append("]");
    return sb.toString();
  }

  // how to handle functions?

}
