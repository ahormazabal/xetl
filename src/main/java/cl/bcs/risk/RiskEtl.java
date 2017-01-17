package cl.bcs.risk;

import cl.bcs.risk.pipeline.Pipeline;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.*;
import java.util.logging.LogManager;
import java.util.stream.Collectors;

/**
 * @author Alberto Hormazabal Cespedes
 * @author exaTech Ingenieria SpA. (info@exatech.cl)
 */
public class RiskEtl {
  private static final Logger LOG = LoggerFactory.getLogger(RiskEtl.class);

  private static final String PIPELINE_PATH = "/pipelines/";

  private final Properties mainProperties;

  //  private final Map<String, Pipeline> availablePipelines;

  private final List<Pipeline> pipelines;

  private final DataSourceManager dataSourceManager;


  /**
   * Construye una nueva instancia del controlador.
   *
   * @param mainProperties propiedades para el controlador.
   * @param pipelines Lista de pipelines a ejecutar.
   * @throws IOException En caso de error de inicializacion.
   */
  public RiskEtl(Properties mainProperties, List<String> pipelines) throws IOException {
    this.mainProperties = mainProperties;
    this.dataSourceManager = new DataSourceManager(this);
    ;

    // Load available pipelines.
//    availablePipelines = new BufferedReader(new InputStreamReader(RiskEtl.class.getResourceAsStream(PIPELINE_PATH))).lines()
//        .map(this::loadPipeline)
//        .collect(Collectors.toMap(Pipeline::getName, pipeline -> pipeline));

    // Get pipelines to run.
    this.pipelines = pipelines.stream()
        .map(s -> {
          Pipeline p = loadPipeline(s);
          if (p == null) {
            throw new IllegalArgumentException("Invalid pipeline requested: " + s);
          }
          return p;
        })
        .collect(Collectors.toList());

  }

  private Pipeline loadPipeline(String filename) {
    String path = PIPELINE_PATH + filename + ".xml";
    try {
      LOG.info("Loading pipeline: " + path);

      InputStream pipeResource = RiskEtl.class.getResourceAsStream(path);
      if (pipeResource == null) {
        throw new IllegalArgumentException("Resource does not exists: " + path);
      }

      XmlMapper xmlMapper = new XmlMapper();
      Pipeline p = xmlMapper.readValue(pipeResource, Pipeline.class);
      p.initialize(this);

      return p;
    } catch (Exception e) {
      throw new RuntimeException("Error loading pipeline: " + path, e);
    }
  }

  public Properties getMainProperties() {
    return mainProperties;
  }

  public DataSourceManager getDataSourceManager() {
    return dataSourceManager;
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
        else {
          LOG.warn(String.format("No value found on properties for expansion ${%s}", key));
        }
      }
    }
    return value;
  }


  public void runPipelines() {
    pipelines.forEach(Pipeline::run);
  }


  public static void main(String[] args) throws Exception {

    if (args.length < 1) {
      printHelp(1);
    }

    // Process arguments.
    Properties argProperties = new Properties();
    List<String> pipes = new ArrayList<>(args.length);

    // process arguments
    for (String s : args) {

      if (s.startsWith("--help")) {
        printHelp(0);
      }

      // Process properties arguments.
      else if (s.startsWith("-D")) {
        // TODO change to use the Properties parser.
        int keyidx = s.indexOf('=');
        if (keyidx < 1 || s.length() <= keyidx) {
          throw new IllegalArgumentException("Not a property: " + s);
        }
        String key = s.substring(2, keyidx).trim();
        String value = s.substring(keyidx + 1).trim();
        argProperties.setProperty(key, value);
      }

      // All other arguments are pipelines.
      else {
        pipes.add(s.trim());
      }
    }

    if (pipes.size() < 1) {
      throw new IllegalArgumentException("No pipelines defined in arguments.");
    }

    LOG.info("Initializing RiskETL...");
    argProperties.store(System.out, "Argument properties");


    // Build context properties.
    Properties properties = new Properties();

    // Add OS environment.
    properties.putAll(System.getenv());

    // Add JVM System properties.
    properties.putAll(System.getProperties());

    // Add Command line properties.
    properties.putAll(argProperties);

    try {
      // Build etl instance.
      RiskEtl etl = new RiskEtl(properties, pipes);

      // Launch!
      etl.runPipelines();
    } catch (Exception e) {
      throw new Exception("Error procesando pipeline. Use --help para ver el uso.", e);
    }

    LOG.info("Process finished successfully.");
  }

  private static void printHelp(int status) {
    System.out.printf(
        "\n" +
            "Usage:\n" +
            "\tjava -jar etl.jar [-Dproperty=value ...] pipeline_name [pipeline_name ...]\n\n" +
            "Properties are taken from command line arguments, java system properties, and system environment. In that order.\n\n" +
            "Default dataSource properties:\n" +
            "DATABASE_URL: JDBC Url\n" +
            "DATABASE_USER: DB Username\n" +
            "DATABASE_PASS: DB User password\n" +
            "\n");

    System.exit(status);
  }

}
