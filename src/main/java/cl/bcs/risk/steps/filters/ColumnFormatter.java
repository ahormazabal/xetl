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
                char convertType = format.charAt(format.length() - 1);
                String oldVal = record.get(entry.getKey());
                String newVal = String.format(Locale.US, format, getConvertionObject(convertType, oldVal));

                //FIX para que conversiones %03d de null queden en "   " y no en "000".
                if(oldVal == null && convertType == 'd'){
                  newVal = newVal.replaceAll(".", " ");
                }

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
   * @param convert
   * @param oldVal
   * @return
   */
  private Object getConvertionObject(char convert, String oldVal) {
    switch (convert) {
      case 's':
        return oldVal == null ? "" : oldVal;
      case 'f':
        return oldVal == null ? BigDecimal.ZERO : new BigDecimal(oldVal);
      case 'd':
        return oldVal == null ? 0 : Integer.valueOf(oldVal);
      default:
        throw new UnsupportedOperationException("Unsupported convertion type: %" + convert);
    }
  }

}
