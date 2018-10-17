package cl.exatech.tools.etl.steps.filters;

import cl.exatech.tools.etl.pipeline.AbstractBaseStep;
import cl.exatech.tools.etl.pipeline.FilterStep;
import cl.exatech.tools.etl.pipeline.Pipeline;
import cl.exatech.tools.etl.pipeline.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.stream.Stream;

/**
 * Este filtro setea todas las columnas a 0 a partiendo de la columna indicada.
 *
 * @author Alberto Hormazabal Cespedes
 * @author exaTech Ingenieria SpA. (info@exatech.cl)
 */
public class ZeroToRightFilter extends AbstractBaseStep
    implements FilterStep {

  private static final Logger LOG = LoggerFactory.getLogger(ZeroToRightFilter.class);

  private int fromColumn;

  @Override
  public String getType() {
    return "zeroToRight";
  }

  @Override
  public void initialize(Pipeline pipeline, Map<String, String> properties) throws Exception {
    super.initialize(pipeline, properties);

    fromColumn = Integer.valueOf(getRequiredProperty("fromColumn"));
  }

  @Override
  public Stream<? extends Record> filter(Stream<? extends Record> recordStream) {
    return recordStream
        .map(Record::mutable)
        .map(record -> {
          for (int i = fromColumn; i < record.size(); i++) {
            record.set(i, "0");
          }
          return record;
        });
  }
}
