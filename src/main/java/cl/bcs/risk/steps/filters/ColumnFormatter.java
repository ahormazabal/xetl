package cl.bcs.risk.steps.filters;

import cl.bcs.risk.pipeline.AbstractBaseStep;
import cl.bcs.risk.pipeline.FilterStep;
import cl.bcs.risk.pipeline.Pipeline;
import cl.bcs.risk.pipeline.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Formateador de columnas.
 *
 * @author Alberto Hormazabal Cespedes
 * @author exaTech Ingenieria SpA. (info@exatech.cl)
 */
public class ColumnFormatter extends AbstractBaseStep
    implements FilterStep {

  private static final Logger LOG = LoggerFactory.getLogger(ColumnFormatter.class);

  private Map<String, String> columnFormats;


  @Override
  public String getType() {
    return "format";
  }

  @Override
  public void initialize(Pipeline pipeline, Map<String, String> properties) throws Exception {
    super.initialize(pipeline, properties);

    columnFormats = properties.entrySet().stream()
        .collect(Collectors.toMap(Map.Entry::getKey, entry -> entry.getValue().trim()));
  }

  @Override
  public Stream<? extends Record> filter(Stream<? extends Record> recordStream) {
    return recordStream
        .map(Record::mutable)
        .map(record -> {
          try {
            columnFormats.entrySet().forEach(entry -> {
              if (record.containsKey(entry.getKey())) {
                String format = entry.getValue();
                String oldVal = record.get(entry.getKey());
                String newVal = formatValue(format, oldVal);
                record.set(entry.getKey(), newVal);
              }
            });
            return record;
          } catch (Exception e) {
            LOG.error("Error procesando registro: " + record.toString());
            throw new RuntimeException(e);
          }
        });
  }

  /**
   * Convierte un string en el tipo de dato apropiado para la conversion.
   *
   * @param format
   * @param oldVal
   * @return
   */
  private String formatValue(String format, String oldVal) {
    if (oldVal == null)
      return "";

    char convert = format.charAt(format.length() - 1);
    switch (convert) {
      case 's':
        return String.format(Locale.US, format, oldVal);
      case 'f':
        return String.format(Locale.US, format, new BigDecimal(oldVal));
      case 'd':
        return String.format(Locale.US, format, Integer.valueOf(oldVal));
      default:
        throw new UnsupportedOperationException("Unsupported convertion type: " + format);
    }
  }

}
