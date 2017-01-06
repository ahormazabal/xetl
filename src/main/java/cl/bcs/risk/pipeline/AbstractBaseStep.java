package cl.bcs.risk.pipeline;

import java.util.Map;
import java.util.Objects;

/**
 * @author Alberto Hormazabal Cespedes
 * @author exaTech Ingenieria SpA. (info@exatech.cl)
 */
public abstract class AbstractBaseStep
    implements Step {

  protected Pipeline pipeline;

  protected Map<String, String> properties;

  @Override
  public abstract String getType();

  @Override
  public void initialize(Pipeline pipeline, Map<String, String> properties) throws Exception {
    this.pipeline = pipeline;
    this.properties = properties;
  }

  public Pipeline getPipeline() {
    return pipeline;
  }

  public void setPipeline(Pipeline pipeline) {
    this.pipeline = pipeline;
  }

  public Map<String, String> getProperties() {
    return properties;
  }

  protected String getRequiredProperty(String key) {
    return Objects.requireNonNull(properties.get(key), key);
  }

  protected String getOptionalProperty(String key, String defaultValue) {
    return properties.getOrDefault(key, defaultValue);
  }

  public void setProperties(Map<String, String> properties) {
    this.properties = properties;
  }
}
