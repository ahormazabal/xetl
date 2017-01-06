package cl.bcs.risk.pipeline;

import java.util.Map;

/**
 * @author Alberto Hormazabal Cespedes
 * @author exaTech Ingenieria SpA. (info@exatech.cl)
 */
public interface Step {

  String getType();

  void initialize(Pipeline pipeline, Map<String, String> properties) throws Exception;

}
