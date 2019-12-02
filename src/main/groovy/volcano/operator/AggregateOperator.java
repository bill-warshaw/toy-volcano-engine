package volcano.operator;

import static java.util.stream.Collectors.toList;
import static volcano.operator.aggregate.AggregateFn.AVG;
import static volcano.operator.aggregate.AggregateFn.COUNT;
import static volcano.operator.aggregate.AggregateFn.SUM;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import com.google.common.base.Strings;

import volcano.db.Row;
import volcano.operator.aggregate.AggregateFn;
import volcano.operator.util.OutputSchema;
import volcano.operator.util.Column;

public class AggregateOperator implements Operator {
  private final Operator input;
  private final List<Column> groupingColumns;
  private final List<Column> aggrColumns;

  private final List<Integer> groupingColIndexes;
  private final OutputSchema outputSchema;

  private final List<HashMap<Object,Object>> aggregates;
  //todo hold grouping col combos

  private final Set<Object> uniqueGroups;
  private Iterator uniqueGroupsToReturn;

  public AggregateOperator(Operator input, List<Column> columns) {
    this.input = input;
    this.groupingColumns = columns.stream().filter(Column::isGrouping).collect(toList());
    this.aggrColumns = columns.stream().filter(c -> c.getFn().isPresent()).collect(toList());
    this.aggregates = new ArrayList<>();
    this.uniqueGroups = new HashSet<>();
    this.groupingColIndexes = groupingColumns.stream()
        .map(gc -> input.getOutputSchema().columnIndex(gc.getName()))
        .collect(toList());
    aggrColumns.forEach(c -> aggregates.add(new HashMap<>()));
    this.outputSchema = new OutputSchema(columns);
  }

  @Override
  public void open() {
    input.open();
    while (true) {
      Row r = input.next();
      if (r == null) {
        break;
      }
      List<Object> groupingElements = groupingColIndexes.stream().map(r::getAt).collect(toList());
      uniqueGroups.add(groupingElements);

      for (int i = 0; i < aggrColumns.size(); i++) {
        Column col = aggrColumns.get(i);
        String colName = col.getName();
        AggregateFn fn = col.getFn().get();
        HashMap<Object,Object> aggrMap = aggregates.get(i);
        Object aggrElem = r.getAt(input.getOutputSchema().columnIndex(colName));
        if (!aggrMap.containsKey(groupingElements)) {
          if (fn == COUNT) {
            aggrMap.put(groupingElements, 1);
          } else if (fn == AVG) {
            aggrMap.put(groupingElements, new Object[] {bigDecimal(aggrElem), 1});
          } else if (fn == SUM) {
            aggrMap.put(groupingElements, bigDecimal(aggrElem));
          } else {
            aggrMap.put(groupingElements, aggrElem);
          }
        } else {
          switch (fn) {
          case AVG:
            Object[] totalAndCount = (Object[])aggrMap.get(groupingElements);
            BigDecimal total = (BigDecimal)totalAndCount[0];
            int count = (int)totalAndCount[1];
            aggrMap.put(groupingElements, new Object[] {total.add(bigDecimal(aggrElem)), count + 1});
            break;
          case COUNT:
            aggrMap.put(groupingElements, ((Integer)aggrMap.get(groupingElements)) + 1);
            break;
          case MIN:
            Comparable c = (Comparable)aggrMap.get(groupingElements);
            if ((c.compareTo(aggrElem)) > 0) {
              aggrMap.put(groupingElements, aggrElem);
            }
            break;
          case MAX:
            Comparable d = (Comparable)aggrMap.get(groupingElements);
            if ((d.compareTo(aggrElem)) < 0) {
              aggrMap.put(groupingElements, aggrElem);
            }
            break;
          case SUM:
            aggrMap.put(groupingElements,
                ((BigDecimal)aggrMap.get(groupingElements)).add(bigDecimal(aggrElem)));
            break;
          default:
            throw new IllegalArgumentException(String.format("Unrecognized aggr fn %s", fn));
          }
        }
      }
    }
    uniqueGroupsToReturn = uniqueGroups.iterator();
    input.close();
  }

  @Override
  public Row next() {
    if (!uniqueGroupsToReturn.hasNext()) {
      return null;
    }
    List<Comparable> group = (List<Comparable>)uniqueGroupsToReturn.next();
    List<Comparable> aggs = new ArrayList<>();
    for (int i = 0; i < aggrColumns.size(); i++) {
      Column col = aggrColumns.get(i);
      Object agg = aggregates.get(i).get(group);
      switch (col.getFn().get()) {
      case AVG:
        aggs.add(((BigDecimal)((Object[])agg)[0]).doubleValue() / (Integer)((Object[])agg)[1]);
        break;
      case COUNT:
      case MAX:
      case MIN:
        aggs.add((Comparable)agg);
        break;
      case SUM:
        aggs.add(((BigDecimal)agg).doubleValue());
        break;
      default:
        throw new IllegalArgumentException(String.format("Unrecognized aggr fn %s", col.getFn().get()));
      }
    }
    group.addAll(aggs);
    return new Row(group);
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
    sb.append("aggregate[");
    sb.append("grouping:");
    sb.append(groupingColumns);
    sb.append(",");
    sb.append("fns:");
    sb.append(aggrColumns);
    sb.append(",");
    sb.append("input:").append("\n");
    sb.append(input.printOperator(indentation + 2));
    sb.append("]");
    return sb.toString();
  }

  private BigDecimal bigDecimal(Object o) {
    if (o instanceof Double) {
      return new BigDecimal((Double)o);
    } else if (o instanceof Integer) {
      return new BigDecimal((Integer)o);
    } else {
      throw new IllegalArgumentException(
          String.format("Attempted to convert %s into BigDecimal", o.getClass()));
    }
  }
}
