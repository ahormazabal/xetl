package cl.exatech.tools.etl.steps.filters;

import cl.exatech.tools.etl.pipeline.AbstractBaseStep;
import cl.exatech.tools.etl.pipeline.FilterStep;
import cl.exatech.tools.etl.pipeline.Pipeline;
import cl.exatech.tools.etl.pipeline.Record;
import cl.exatech.tools.etl.utils.DateUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Map;
import java.util.stream.Stream;

/**
 * Filtro para cambio de formato de fecha.
 *
 * @author Alberto Hormazabal Cespedes
 * @author exaTech Ingenieria SpA. (info@exatech.cl)
 */
public class DateFormatFilter extends AbstractBaseStep
    implements FilterStep {

  private static final Logger LOG = LoggerFactory.getLogger(DateFormatFilter.class);

  private String column;
  private String from;
  private String to;

  private SimpleDateFormat sdfIn;
  private SimpleDateFormat sdfOut;

  @Override
  public String getType() {
    return "dateformat";
  }

  @Override
  public void initialize(Pipeline pipeline, Map<String, String> properties) throws Exception {
    super.initialize(pipeline, properties);

    column = getRequiredProperty("column").toLowerCase().trim();
    from = getRequiredProperty("from");
    to = getRequiredProperty("to");

    sdfIn = new SimpleDateFormat(from);
    sdfOut = new SimpleDateFormat(to);
  }

  private String performReplace(String val) throws ParseException {
    return DateUtils.formatDate(val, sdfIn, sdfOut);
  }

  @Override
  public Stream<? extends Record> filter(Stream<? extends Record> recordStream) {
    return recordStream
        .map(Record::mutable)
        .map(record -> {
          String value = record.get(column);

          if (value == null || value.isEmpty()) {
            return record;
          }

          try {
            record.set(column, performReplace(value));
          } catch (Exception e) {
            throw new RuntimeException(String.format("Error parsing date <%s=%s> on record [%s] : %s",
                column,
                value,
                record,
                e.getMessage()
            ), e);
          }
          return record;
        });
  }
}
