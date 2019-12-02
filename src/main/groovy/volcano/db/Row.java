package volcano.db;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class Row {

  private final List<? extends Comparable> elements;

  public Row(List<? extends Comparable> elements) {
    this.elements = elements;
  }

  public Comparable getAt(int index) {
    return elements.get(index);
  }

  public int size() {
    return elements.size();
  }

  public Row combine(Row otherRow) {
    List<Comparable> l = new ArrayList<>();
    l.addAll(elements);
    l.addAll(otherRow.elements);
    return new Row(l);
  }

  @Override
  public String toString() {
    return elements.stream().map(e -> {
      if (e instanceof String) {
        return "'" + e + "'";
      } else {
        return ((Comparable)e).toString();
      }
    }).collect(Collectors.joining(","));
  }

  @Override
  public boolean equals(Object o) {
    if (this == o)
      return true;
    if (o == null || getClass() != o.getClass())
      return false;
    Row row = (Row)o;
    return Objects.equals(elements, row.elements);
  }

  @Override
  public int hashCode() {
    return Objects.hash(elements);
  }
}
