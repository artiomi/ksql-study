package my.study.udf;

import io.confluent.ksql.function.udf.Udf;
import io.confluent.ksql.function.udf.UdfDescription;
import io.confluent.ksql.function.udf.UdfParameter;
import java.util.Map;
import org.apache.kafka.common.Configurable;

@UdfDescription(name = "scalar_formula",
    author = "example user",
    version = "1.0.2",
    description = "A custom formula for important business logic.")
public class ScalarUdf implements Configurable {

  private int baseValue;

  @Override
  public void configure(final Map<String, ?> map) {
    String s = (String) map.get("ksql.functions.formula.base.value");
    baseValue = s != null ? Integer.parseInt(s) : 11;
  }

  @Udf(description = "The standard version of the formula with integer parameters.")
  public long formula(@UdfParameter int v1, @UdfParameter int v2) {
    return ((long) v1 * v2) + baseValue;
  }

  @Udf(description = "A special variant of the formula, handling double parameters.")
  public long formula(@UdfParameter double v1, @UdfParameter double v2) {
    return ((int) (Math.ceil(v1) * Math.ceil(v2))) + baseValue;
  }
}
