package my.study.udf;

import io.confluent.ksql.function.udf.UdfParameter;
import io.confluent.ksql.function.udtf.Udtf;
import io.confluent.ksql.function.udtf.UdtfDescription;
import java.util.ArrayList;
import java.util.List;

@UdtfDescription(name = "tabular_formula",
    author = "example user",
    version = "1.5.0",
    description = "Disassembles a sequence and produces new elements concatenated with indices.")
public class TabularUdtf {

  private static final String DELIMITER = "-";

  @Udtf(description = "Takes an array of any type and returns rows with each element paired to its index.")
  public <E> List<String> indexSequence(@UdfParameter List<E> x) {
    List<String> result = new ArrayList<>();

    for (int i = 0; i < x.size(); i++) {
      result.add(x.get(i) + DELIMITER + i);
    }

    return result;
  }
}
