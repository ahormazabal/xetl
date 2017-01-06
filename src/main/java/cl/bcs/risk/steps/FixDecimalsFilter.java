package cl.bcs.risk.steps;

import cl.bcs.risk.pipeline.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Stream;

/**
 * @author Alberto Hormazabal Cespedes
 * @author exaTech Ingenieria SpA. (info@exatech.cl)
 */
public class FixDecimalsFilter extends AbstractBaseStep
    implements FilterStep {

  private static final Logger LOG = LoggerFactory.getLogger(FixDecimalsFilter.class);

  private String from;
  private String to;

  private String replaceRegex;
  private String replacement;

  @Override
  public String getType() {
    return "fixdecimals";
  }

  @Override
  public void initialize(Pipeline pipeline, Map<String, String> properties) throws Exception {
    super.initialize(pipeline, properties);

    from = getRequiredProperty("from");
    to = getRequiredProperty("to");

    if (from.length() != 1 || to.length() != 1) {
      throw new IllegalArgumentException("decimal separators must be only one character");
    }

    replaceRegex = "([0-9]+)" + from + "([0-9]+)";
    replacement = "$1" + to + "$2";
  }

  private String performReplace(String val) {
    return val == null ? null : val.replaceAll(replaceRegex, replacement);
  }

  @Override
  public Stream<? extends Record> filter(Stream<? extends Record> recordStream) {
    return recordStream
        .map(Record::mutable)
        .map(record -> {
          record.entrySet().forEach(entry -> {
            entry.setValue(performReplace(entry.getValue()));
          });
          return record;
        });
  }
}
