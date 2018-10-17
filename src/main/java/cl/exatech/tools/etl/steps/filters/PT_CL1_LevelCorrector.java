package cl.exatech.tools.etl.steps.filters;

import cl.exatech.tools.etl.pipeline.AbstractBaseStep;
import cl.exatech.tools.etl.pipeline.FilterStep;
import cl.exatech.tools.etl.pipeline.Pipeline;
import cl.exatech.tools.etl.pipeline.Record;
import cl.exatech.tools.etl.utils.MutableRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.stream.Stream;

/**
 * Como las condiciones del nivel PT y el nivel CL1 son equivalentes, esto causa que al extraer ambos
 * niveles via query SQL se pierda el nivel CL1.
 *
 * Este filtro implementa las correcciones necesarias para duplicar los registros nivel PT y agregarlos
 * al stream con nivel CL, ademas de hacer algunos ajustes de datos.
 *
 * En pocas palabras, por cada registro de nivel 'PT' se agregara un segundo registro de nivel 'CL' equivalente.
 *
 * @author Alberto Hormazabal Cespedes
 * @author exaTech Ingenieria SpA. (info@exatech.cl)
 */
public class PT_CL1_LevelCorrector extends AbstractBaseStep
    implements FilterStep {

  private static final Logger LOG = LoggerFactory.getLogger(PT_CL1_LevelCorrector.class);

  private static final String LEVEL_FIELD    = "nivel";
  private static final String CL_FIELD       = "cl";
  private static final String PT_LEVEL_LABEL = "PT";
  private static final String CL_LEVEL_LABEL = "CL";


  @Override
  public String getType() {
    return "pt_cl1_LevelCorrector";
  }

  @Override
  public void initialize(Pipeline pipeline, Map<String, String> properties) throws Exception {
    super.initialize(pipeline, properties);
  }

  @Override
  public Stream<? extends Record> filter(Stream<? extends Record> recordStream) {
    LOG.info("Applying PT-CL1 level correction.");
    return recordStream
        .map(Record::mutable)
        .flatMap(record -> {
          if (PT_LEVEL_LABEL.equals(record.get(LEVEL_FIELD))) {

            // Crear registro CL
            MutableRecord newRecord = new MutableRecord(record);
            newRecord.set(LEVEL_FIELD, CL_LEVEL_LABEL);

            // Borrar columna CL en registro original.
            record.set(CL_FIELD, null);

            // Agregar registro nuevo al stream.
            return Stream.of(record, newRecord);
          } else {
            // Registro normal devolver sin cambios.
            return Stream.of(record);
          }
        });
  }
}
