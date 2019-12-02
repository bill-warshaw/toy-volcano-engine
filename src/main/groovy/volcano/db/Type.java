package volcano.db;

public enum Type {
  BOOLEAN, DOUBLE, INT, STRING;

  // needed to handle values from SQL service being mis-typed by GSON
  // could use Jackson to avoid this
  public Comparable cast(Object o) {
    if (this == DOUBLE) {
      if (o instanceof Integer) {
        return ((Integer)o).doubleValue();
      }
      return (double)o;
    } else if (this == INT) {
      if (o instanceof Double) {
        return ((Double)o).intValue();
      }
      return (int)o;
    } else if (this == BOOLEAN) {
      return (Boolean)o;
    } else {
      return o.toString();
    }
  }
}
