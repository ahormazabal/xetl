package cl.bcs.risk.pipeline;

import java.util.stream.Stream;

/**
 * Step inicial para una cadena de steps.
 *
 * Los steps que implementan esta interfaz son el primer paso de cualquier pipeline
 * y quienes producen los datos de entrada.
 *
 * @author Alberto Hormazabal Cespedes
 * @author exaTech Ingenieria SpA. (info@exatech.cl)
 */
public interface BeginStep extends Step{

  Stream<? extends Record> begin();

}
