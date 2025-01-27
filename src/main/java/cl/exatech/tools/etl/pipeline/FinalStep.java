package cl.exatech.tools.etl.pipeline;

import java.util.stream.Stream;

/**
 * Step final en el que se consumen los datos del pipeline.
 *
 *
 * @author Alberto Hormazabal Cespedes
 * @author exaTech Ingenieria SpA. (info@exatech.cl)
 */
public interface FinalStep extends Step{

  void finish(Stream<? extends Record> recordStream);

}
