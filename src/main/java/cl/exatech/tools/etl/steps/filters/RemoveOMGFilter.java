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
 * Remueve los campos agregados por {@link OMGFilenameFilter}.
 *
 * @author Alberto Hormazabal Cespedes
 * @author exaTech Ingenieria SpA. (info@exatech.cl)
 */
public class RemoveOMGFilter extends AbstractBaseStep
    implements FilterStep {

  private static final Logger LOG = LoggerFactory.getLogger(RemoveOMGFilter.class);

  @Override
  public String getType() {
    return "removeOMG";
  }

  @Override
  public void initialize(Pipeline pipeline, Map<String, String> properties) throws Exception {
    super.initialize(pipeline, properties);
  }

  @Override
  public Stream<? extends Record> filter(Stream<? extends Record> recordStream) {
    return recordStream
        .map(Record::mutable)
        .map(record -> {
          if (record.containsKey(OMGFilenameFilter.DATE_FIELD)
              && record.containsKey(OMGFilenameFilter.INTRADAY_FIELD)) {
            record.remove(OMGFilenameFilter.DATE_FIELD);
            record.remove(OMGFilenameFilter.INTRADAY_FIELD);
          }
          return record;
        });
  }
}
