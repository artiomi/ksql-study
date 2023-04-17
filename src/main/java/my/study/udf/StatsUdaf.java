package my.study.udf;

import io.confluent.ksql.function.udaf.Udaf;
import io.confluent.ksql.function.udaf.UdafDescription;
import io.confluent.ksql.function.udaf.UdafFactory;
import java.util.Objects;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;

@UdafDescription(name = "stats_formula",
    author = "example user",
    version = "1.3.5",
    description = "Maintains statistical values.")
public class StatsUdaf {

  public static final Schema PARAM_SCHEMA = SchemaBuilder.struct().optional()
      .field("C", Schema.OPTIONAL_INT64_SCHEMA)
      .build();

  public static final String PARAM_SCHEMA_DESCRIPTOR = "STRUCT<" +
      "C BIGINT" +
      ">";

  public static final Schema AGGREGATE_SCHEMA = SchemaBuilder.struct().optional()
      .field("MIN", Schema.OPTIONAL_INT64_SCHEMA)
      .field("MAX", Schema.OPTIONAL_INT64_SCHEMA)
      .field("COUNT", Schema.OPTIONAL_INT64_SCHEMA)
      .build();

  public static final String AGGREGATE_SCHEMA_DESCRIPTOR = "STRUCT<" +
      "MIN BIGINT," +
      "MAX BIGINT," +
      "COUNT BIGINT" +
      ">";

  public static final Schema RETURN_SCHEMA = SchemaBuilder.struct().optional()
      .field("MIN", Schema.OPTIONAL_INT64_SCHEMA)
      .field("MAX", Schema.OPTIONAL_INT64_SCHEMA)
      .field("COUNT", Schema.OPTIONAL_INT64_SCHEMA)
      .field("DIFFERENTIAL", Schema.OPTIONAL_INT64_SCHEMA)
      .build();

  public static final String RETURN_SCHEMA_DESCRIPTOR = "STRUCT<" +
      "MIN BIGINT," +
      "MAX BIGINT," +
      "COUNT BIGINT," +
      "DIFFERENTIAL BIGINT" +
      ">";

  private StatsUdaf() {
  }

  //TODO for some reasons Ksql is not able to parse value from paramSchema, it may be related to ksql java
  @UdafFactory(description = "Computes the min, max, count, and difference between min/max.",
//      paramSchema = PARAM_SCHEMA_DESCRIPTOR,
      aggregateSchema = AGGREGATE_SCHEMA_DESCRIPTOR,
      returnSchema = RETURN_SCHEMA_DESCRIPTOR
  )
  public static Udaf<Integer, Struct, Struct> createUdaf() {
    return new StatsUdafImpl();
  }

  private static class StatsUdafImpl implements Udaf<Integer, Struct, Struct> {

    @Override
    public Struct initialize() {
      return new Struct(AGGREGATE_SCHEMA);
    }

    @Override
    public Struct aggregate(Integer newValue, Struct aggregateValue) {
//      long c = newValue.getInt64("C");
      long c = newValue;

      long min = Math.min(c, getMin(aggregateValue));
      long max = Math.max(c, getMax(aggregateValue));
      long count = (getCount(aggregateValue) + 1);

      aggregateValue.put("MIN", min);
      aggregateValue.put("MAX", max);
      aggregateValue.put("COUNT", count);

      return aggregateValue;
    }

    @Override
    public Struct map(Struct intermediate) {
      Struct result = new Struct(RETURN_SCHEMA);

      long min = intermediate.getInt64("MIN");
      long max = intermediate.getInt64("MAX");

      result.put("MIN", min);
      result.put("MAX", max);
      result.put("COUNT", intermediate.getInt64("COUNT"));
      result.put("DIFFERENTIAL", max - min);

      return result;
    }

    @Override
    public Struct merge(Struct aggOne, Struct aggTwo) {
      return aggOne;
    }

    private Long getMin(Struct aggregateValue) {

      return Objects.requireNonNullElse(aggregateValue.getInt64("MIN"), Long.MAX_VALUE);
    }

    private Long getMax(Struct aggregateValue) {

      return Objects.requireNonNullElse(aggregateValue.getInt64("MAX"), Long.MIN_VALUE);
    }

    private Long getCount(Struct aggregateValue) {

      return Objects.requireNonNullElse(aggregateValue.getInt64("COUNT"), 0L);
    }
  }
}

