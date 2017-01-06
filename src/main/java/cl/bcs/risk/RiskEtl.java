package cl.bcs.risk;

import cl.bcs.risk.pipeline.Pipeline;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

/**
 * @author Alberto Hormazabal Cespedes
 * @author exaTech Ingenieria SpA. (info@exatech.cl)
 */
public class RiskEtl {

  private static final String PIPELINE_PATH = "/pipelines/";

  private final Properties mainProperties;

  private final String[] runPipelines;

  private final Map<String, Pipeline> availablePipelines;

  private final List<Pipeline> pipelines;


  public RiskEtl(Properties mainProperties, String[] runPipelines) {
    this.mainProperties = mainProperties;
    this.runPipelines = runPipelines;

    availablePipelines = new BufferedReader(new InputStreamReader(RiskEtl.class.getResourceAsStream(PIPELINE_PATH))).lines()
        .map(this::loadPipeline)
        .collect(Collectors.toMap(Pipeline::getName, pipeline -> pipeline));

    pipelines = Arrays.stream(this.runPipelines)
        .map(s -> {
          Pipeline p = availablePipelines.get(s);
          if (p == null) {
            throw new IllegalArgumentException("Invalid pipeline requested: " + s);
          }
          return p;
        })
        .collect(Collectors.toList());

  }

  private Pipeline loadPipeline(String filename) {
    String path = PIPELINE_PATH + "/" + filename;
    try {

      XmlMapper xmlMapper = new XmlMapper();
      Pipeline p = xmlMapper.readValue(RiskEtl.class.getResourceAsStream(path), Pipeline.class);
      p.setContext(this);
      p.initialize();

      return p;
    } catch (Exception e) {
      throw new RuntimeException("Error loading pipeline: " + path, e);
    }
  }

  public Properties getMainProperties() {
    return mainProperties;
  }

  /**
   * Expands variables marked with ${key} to "value" from the values
   * present on mainProperties.
   *
   * @return
   */
  public String expandProperties(String value) {
    int index = 0;
    while (index < value.length()) {

      int i = value.indexOf("${", index++);
      int e = value.indexOf("}", i);
      if (e > (i + 1) && i >= 0) {
        String key = value.substring(i + 2, e);
        String replacement = mainProperties.getProperty(key);
        if (replacement != null) {
          value = value.replaceAll("\\$\\{" + key + "\\}", replacement);
        }
      }
    }
    return value;
  }


  public void runPipelines() {
    pipelines.forEach(Pipeline::run);
  }



  public static void main(String[] args) {

    Properties properties = new Properties();

    // Add OS environment.
    properties.putAll(System.getenv());

    // Add JVM System properties.
    properties.putAll(System.getProperties());

    RiskEtl etl = new RiskEtl(properties, args);
    etl.runPipelines();

    System.out.println("END");
  }


}
