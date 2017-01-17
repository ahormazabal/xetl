package cl.bcs.risk.steps;

import cl.bcs.risk.pipeline.AbstractBaseStep;
import cl.bcs.risk.pipeline.FilterStep;
import cl.bcs.risk.pipeline.Pipeline;
import cl.bcs.risk.pipeline.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.stream.Stream;

/**
 * Reemplaza el campo "valorizacion" de un registro, por el valor del campo
 * "m_spot_price" si este se encuentra y no es null.
 * <p>Como paso posterior, el campo "m_spot_price" sera removido.</p>
 *
 * <p>Este proceso es para combinar el archivo M con el archivo de Parametros.</p>
 *
 * @author Alberto Hormazabal Cespedes
 * @author exaTech Ingenieria SpA. (info@exatech.cl)
 */
public class InstrumentMValuationFilter extends AbstractBaseStep
    implements FilterStep {

  private static final Logger LOG = LoggerFactory.getLogger(InstrumentMValuationFilter.class);

  private static final String M_FIELD = "m_spot_price";
  private static final String V_FIELD = "valorizacion";

  @Override
  public String getType() {
    return "mValuation";
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
          if (record.containsKey(M_FIELD) && record.containsKey(V_FIELD)) {
            String mspot = record.get(M_FIELD);
            if (mspot != null && !mspot.trim().isEmpty()) {
              record.set(V_FIELD, mspot);
            }
            record.remove(M_FIELD);
          }
          return record;
        });
  }
}
