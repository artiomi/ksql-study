package my.study.udf;

import io.confluent.ksql.function.udaf.Udaf;
import io.confluent.ksql.function.udaf.UdafDescription;
import io.confluent.ksql.function.udaf.UdafFactory;
import java.util.LinkedList;
import java.util.List;

@UdafDescription(name = "agg_formula",
    author = "artiomi",
    version = "1.0.2",
    description = "A custom formula for important business logic.")
public class AggregationUdaf {

  private AggregationUdaf() {
  }
  @UdafFactory(description = "Sums the previous 3 integers of a stream, discarding the oldest elements as new ones arrive.")
  public static Udaf<Integer, List<Integer>, Integer> createUdaf() {
    return new RollingSumUdafImpl();
  }

  private static class RollingSumUdafImpl implements Udaf<Integer, List<Integer>, Integer> {

    private static final int CAPACITY = 3;

    @Override
    public List<Integer> initialize() {
      return new LinkedList<>();
    }

    @Override
    public List<Integer> aggregate(Integer newValue, List<Integer> aggregateValue) {
      aggregateValue.add(newValue);

      if (aggregateValue.size() > CAPACITY) {
        aggregateValue = aggregateValue.subList(1, CAPACITY + 1);
      }

      return aggregateValue;
    }

    @Override
    public Integer map(List<Integer> intermediate) {
      return intermediate.stream().reduce(0, Integer::sum);
    }

    @Override
    public List<Integer> merge(List<Integer> aggOne, List<Integer> aggTwo) {
      return aggTwo;
    }
  }
}
