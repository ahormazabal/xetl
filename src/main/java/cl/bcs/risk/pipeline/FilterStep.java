package cl.bcs.risk.pipeline;

import java.util.stream.Stream;

/**
 * Filter Step
 *
 * Step para filtrar o convertir Records.
 *
 * Estos Step deben estar en la seccion intermedia de un pipeline, es decir
 * antedecido por un BeginStep o un FilterStep, y seguido por un FilterStep o un FinalStep
 *
 * @author Alberto Hormazabal Cespedes
 * @author exaTech Ingenieria SpA. (info@exatech.cl)
 */
public interface FilterStep extends Step{

  Stream<? extends Record> filter(Stream<? extends Record> recordStream);

}
