package volcano.operator.filter;

public enum FilterLogicalOp {
  EQ("="), NEQ("<>"), GT(">"), GTE(">="), LT("<"), LTE("<="), BETWEEN("between"), LIKE("like"), IN("in");

  private final String sqlOp;

  FilterLogicalOp(String sqlOp) {
    this.sqlOp = sqlOp;
  }

  public static FilterLogicalOp findBySqlOp(String sqlOp) {
    for (FilterLogicalOp op : values()) {
      if (op.sqlOp.equalsIgnoreCase(sqlOp)) {
        return op;
      }
    }
    throw new IllegalArgumentException(String.format("Unrecognized SQL operator: %s", sqlOp));
  }
}
