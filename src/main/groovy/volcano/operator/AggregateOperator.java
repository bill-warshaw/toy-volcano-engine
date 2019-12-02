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
import volcano.db.Type;
import volcano.operator.aggregate.AggregateFn;
import volcano.operator.util.OutputSchema;

public class AggregateOperator implements Operator {
  private final Operator input;
  private final List<String> groupingColumns;
  private final List<String> columnsToProject;
  private final List<String> aggrFns;

  private final List<Integer> groupingColIndexes;
  private final OutputSchema outputSchema;

  private final List<HashMap<Object,Object>> aggregates;
  //todo hold grouping col combos

  private final Set<Object> uniqueGroups;
  private Iterator uniqueGroupsToReturn;

  public AggregateOperator(
      Operator input, List<String> groupingColumns, List<String> columnsToProject, List<String> aggrFns) {
    this.input = input;
    this.groupingColumns = groupingColumns;
    this.columnsToProject = columnsToProject;
    this.aggrFns = aggrFns;
    this.aggregates = new ArrayList<>();
    this.uniqueGroups = new HashSet<>();
    this.groupingColIndexes = groupingColumns.stream()
        .map(gc -> input.getOutputSchema().columnIndex(gc))
        .collect(toList());
    aggrFns.forEach(c -> aggregates.add(new HashMap<>()));

    List<String> columns = new ArrayList<>();
    groupingColumns.forEach(columns::add);
    columnsToProject.forEach(columns::add);
    List<Type> columnTypes = new ArrayList<>();
    //todo figure out grouping col types
    groupingColumns.forEach(c -> columnTypes.add(Type.STRING));
    aggrFns.forEach(c -> {
      if (AggregateFn.valueOf(c) == AVG) {
        columnTypes.add(Type.DOUBLE);
      } else {
        //todo figure out types (e.g. string?)
        columnTypes.add(Type.INT);
      }
    });
    this.outputSchema = new OutputSchema(columns, columnTypes);
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

      for (int i = 0; i < aggrFns.size(); i++) {
        String col = columnsToProject.get(i);
        AggregateFn fn = AggregateFn.valueOf(aggrFns.get(i));
        HashMap<Object,Object> aggrMap = aggregates.get(i);
        Object aggrElem = r.getAt(input.getOutputSchema().columnIndex(col));
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
    for (int i = 0; i < aggrFns.size(); i++) {
      Object agg = aggregates.get(i).get(group);
      switch (AggregateFn.valueOf(aggrFns.get(i))) {
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
        throw new IllegalArgumentException(String.format("Unrecognized aggr fn %s", aggrFns.get(i)));
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
    sb.append("fns:[");
    for (int i = 0; i < aggrFns.size(); i++) {
      sb.append(aggrFns.get(i));
      sb.append("(");
      sb.append(columnsToProject.get(i));
      sb.append(")");
      if (i != aggrFns.size() - 1) {
        sb.append(",");
      }
    }
    sb.append("]");
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
