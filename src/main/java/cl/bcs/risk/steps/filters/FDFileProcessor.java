package cl.bcs.risk.steps.filters;

import cl.bcs.risk.pipeline.AbstractBaseStep;
import cl.bcs.risk.pipeline.FilterStep;
import cl.bcs.risk.pipeline.Pipeline;
import cl.bcs.risk.pipeline.Record;
import cl.bcs.risk.utils.MutableRecord;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
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
public class FDFileProcessor extends AbstractBaseStep
    implements FilterStep {

  private static final Logger       LOG  = LoggerFactory.getLogger(FDFileProcessor.class);
  private static final ObjectMapper JSON = new ObjectMapper();

  static final         String LEVEL_FIELD     = "nivel";
  static final         String CM_FIELD        = "cm";
  static final         String PT_FIELD        = "pt";
  static final         String SCENARIOS_FIELD = "escenarios_total";
  static final         String RIESGO_TT_KEY   = "riesgott";
  private static final int    RIESGO_TT_INDEX = 7;


  @Override
  public String getType() {
    return "fdFileProcessor";
  }

  @Override
  public void initialize(Pipeline pipeline, Map<String, String> properties) throws Exception {
    super.initialize(pipeline, properties);
  }

  @Override
  public Stream<? extends Record> filter(Stream<? extends Record> recordStream) {
    LOG.info("Processing FD File Data.");

    Map<String, List<Entry>> workGroups = recordStream
        .map(Record::mutable)

        // Obtener escenarios
        .map(Entry::new)

        // Agrupar
        .collect(Collectors.groupingBy(Entry::groupKey));

    // Determinar Escenario mas grande

    return workGroups.entrySet().stream()
        .flatMap(entry -> {
          String key = entry.getKey();
          List<Entry> entries = entry.getValue();
          int size = entries.isEmpty() ? 0 : entries.get(0).length();
          LOG.info("Processing group: " + key + " size " + size);

          int maxIdx = Integer.MIN_VALUE;
          double maxValue = Double.MIN_VALUE;

          for (int i = 0; i < size; i++) {
            double sum = scenarioValue(i, entries);
            if (sum > maxValue) {
              maxIdx = i;
              maxValue = sum;
            }
          }

          final int maxIndex = maxIdx;

          // Convertir y desagrupar.
          return entries.stream()
              .map(entry1 -> {
                MutableRecord record = entry1.record;
                // Setear resultado en registros
                record.insert(RIESGO_TT_INDEX, RIESGO_TT_KEY, String.valueOf(entry1.evalues()[maxIndex]));
                return record;
              });
        })
        ;
  }


  /**
   * Calcula la suma de los dos valores mas grandes de un escenario.
   *
   * @param scenarioIndex Indice del escenario.
   * @param entries Evalues.
   * @return
   */
  static double scenarioValue(int scenarioIndex, List<Entry> entries) {

    if (entries.size() == 0) {
      return 0;
    } else if (entries.size() == 1) {
      return entries.get(0).evalues()[scenarioIndex] < 0 ? 0 : entries.get(0).evalues()[scenarioIndex];
    }

    double largestA = Double.MIN_VALUE;
    double largestB = Double.MIN_VALUE;

    for (Entry e : entries) {
      Double[] evalues = e.evalues();
      if (evalues[scenarioIndex] > largestA) {
        largestB = largestA;
        largestA = evalues[scenarioIndex];
      } else if (evalues[scenarioIndex] > largestB) {
        largestB = evalues[scenarioIndex];
      }
    }
    return (largestA < 0 ? 0 : largestA) + (largestB < 0 ? 0 : largestB);
  }


  /**
   * Registo decodificado.
   */
  static class Entry {
    private MutableRecord record;
    private String        nivel;
    private String        camara;
    private String        pt;
    private transient Double[] eval_array = null;

    Entry(MutableRecord r) {
      // Set data
      this.record = r;
      this.nivel = r.get(LEVEL_FIELD);
      this.camara = r.get(CM_FIELD);
      this.pt = r.get(PT_FIELD);
    }

    Entry(String nivel, String camara, Double[] eval_array) {
      this.nivel = nivel;
      this.camara = camara;
      this.eval_array = eval_array;
    }

    String groupKey() {
      return nivel + "-" + camara;
    }

    int length() {
      return evalues().length;
    }

    Double[] evalues() {
      if (eval_array == null) {
        // Parse values.
        try {
          this.eval_array = JSON.readValue(this.record.remove(SCENARIOS_FIELD), Double[].class);
        } catch (Exception e) {
          throw new RuntimeException(e.getMessage(), e);
        }
      }
      return eval_array != null ? eval_array : new Double[0];
    }
  }

}


