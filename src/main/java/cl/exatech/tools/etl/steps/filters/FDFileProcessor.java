package cl.exatech.tools.etl.steps.filters;

import cl.exatech.tools.etl.pipeline.AbstractBaseStep;
import cl.exatech.tools.etl.pipeline.FilterStep;
import cl.exatech.tools.etl.pipeline.Pipeline;
import cl.exatech.tools.etl.pipeline.Record;
import cl.exatech.tools.etl.utils.MutableRecord;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Filtro para generacion de la columna "riesgott" del archivo FD.
 *
 * Este filtro hace un preproceso de los datos cargados y determina el escenario que contiene
 * los dos valores mas altos para todos los registros de una camara.
 *
 * Luego reemplaza en cada registro el evalue correspondiente a ese escenario de acuerdo
 * al formato del archivo FD.
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
  static final         String RIESGO_T_KEY    = "riesgot";
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

    // Agrupar data previo a proceso.
    Map<String, List<Entry>> workGroups = recordStream
        .map(Record::mutable)
        .map(Entry::new)
        .collect(Collectors.groupingBy(Entry::groupKey));


    // Generar salida
    return workGroups.entrySet().stream()
        .flatMap(mapEntry -> {
          List<Entry> entries = mapEntry.getValue();
          int size = entries.isEmpty() ? 0 : entries.get(0).length();

          // Determinar Escenario mas grande
          LOG.info("Processing group: " + mapEntry.getKey() + " size " + size);
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

          // Desagrupar datos y generar registros finales.
          return entries
              .stream()
              .map(e -> {
                    // String value = maxIndex < 0 ? "0" : String.valueOf(e.evalues()[maxIndex]);
                    Double val = maxIndex < 0 ? 0 : e.evalues()[maxIndex];
                    String value = val > 0 ? String.valueOf(val) : "0";
                    return e.record
                        .append(RIESGO_TT_KEY, value)
                        .append(RIESGO_T_KEY, value);
                  }
              );
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
    private           MutableRecord record;
    private           String        nivel;
    private           String        camara;
    private           String        pt;
    private transient Double[]      eval_array = null;

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
      if (eval_array != null) {
        return eval_array;
      } else {
        // Parse values.
        try {
          // Parseamos evalues y eliminamos data del record.
          this.eval_array = Optional
              .ofNullable(JSON.readValue(this.record.remove(SCENARIOS_FIELD), Double[].class))
              .orElse(new Double[0]);
        } catch (Exception e) {
          throw new RuntimeException(e.getMessage(), e);
        }
      }
      return eval_array;
    }
  }

}


