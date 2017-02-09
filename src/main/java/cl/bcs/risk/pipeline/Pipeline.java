package cl.bcs.risk.pipeline;

import cl.bcs.risk.RiskEtl;
import cl.bcs.risk.StepRegister;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Contenedor de un pipeline.
 *
 * Esta clase es intanciada por {@link RiskEtl}
 * (via {@link com.fasterxml.jackson.dataformat.xml.XmlMapper}) al parsear el xml de definicion.
 *
 * @author Alberto Hormazabal Cespedes
 * @author exaTech Ingenieria SpA. (info@exatech.cl)
 */
public class Pipeline
    implements Runnable {
  private static final Logger LOG = LoggerFactory.getLogger(Pipeline.class);

  /** Nombre del pipeline. */
  private String name;

  /** Listado ordenado con las propiedades de todos los steps del pipeline. */
  private List<Map<String, String>> stepProperties;

  private transient List<Step> pipelineSteps;
  private transient RiskEtl    context;
  private transient volatile boolean executed = false;

  /**
   * Construye un nuevo pipeline basico.
   * <p>Para usar este pipeline primero se debe setear el valor de {@link #setName(String)}</p>
   */
  public Pipeline() {
    this.pipelineSteps = new LinkedList<>();
  }

  public Pipeline(String name, List<Map<String, String>> stepProperties) {
    this.name = name;
    this.stepProperties = stepProperties;
  }

  /**
   * Inicializa el pipeline a partir de la informacion en {@link #name} y {@link #stepProperties}.
   *
   * @throws Exception
   */
  public void initialize(RiskEtl context) throws Exception {

    //Set execution context.
    this.context = context;

    // Build steps.
    for (Map<String, String> sp : stepProperties) {

      // Expand context into step properties.
      Map<String, String> stepProps = sp.entrySet().stream()
          .map(entry -> {
            entry.setValue(context.expandProperties(entry.getValue()));
            return entry;
          })
          .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

      // Initialize Step.
      String type = stepProps.get("type");
      if (type == null) {
        throw new IllegalArgumentException("Step type not found.");
      }

      // Create and add step.
      pipelineSteps.add(StepRegister.initStep(type, stepProps, this));
    }

    validate();
  }


  /**
   * Validates the pipeline structure, returning true if the pipeline is correct, or
   * throwing an IllegalStateException if an error is encountered.
   *
   * @return
   */
  private boolean validate() {
    Iterator<Step> iter = pipelineSteps.iterator();

    if (!iter.hasNext())
      throw new IllegalStateException(name + ": Empty pipeline");

    if (!BeginStep.class.isAssignableFrom(iter.next().getClass())) {
      throw new IllegalStateException(name + ": First step is not a BeginStep");
    }

    if (!iter.hasNext()) {
      throw new IllegalStateException(name + ": Incomplete pipeline. No finalizer.");
    }

    do {
      Step nextStep = iter.next();
      // Is final one ?
      if (iter.hasNext()) {
        if (!FilterStep.class.isAssignableFrom(nextStep.getClass())) {
          throw new IllegalStateException(name + ": Invalid pipeline: Step is not a filter: " + nextStep.getClass().getName());
        }
      } else {
        if (!FinalStep.class.isAssignableFrom(nextStep.getClass())) {
          throw new IllegalStateException(name + ": Incomplete pipeline. No finalizer.");
        }
      }
    }
    while (iter.hasNext());

    return true;
  }


  /**
   * Executes the pipeline.
   *
   * @return
   */
  public void run() {
    if (executed) {
      throw new IllegalStateException("Already executed");
    }
    executed = true;
    validate();

    LOG.info("Running pipeline: " + name);

    // Exec Begin
    Iterator<Step> iter = pipelineSteps.iterator();
    Step nextStep = iter.next();

    // Run Begin Step
    Stream<? extends Record> pipelineStream = ((BeginStep) nextStep).begin();

    do {
      nextStep = iter.next();
      if (iter.hasNext()) {
        // Run filters
        pipelineStream = ((FilterStep) nextStep).filter(pipelineStream);
      } else {
        // Exec Finish
        ((FinalStep) nextStep).finish(pipelineStream);
      }
    }
    while (iter.hasNext());

    // Close stream
    pipelineStream.close();
  }

  public RiskEtl getContext() {
    return context;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }


  public List<Map<String, String>> getSteps() {
    return stepProperties;
  }

  public void setSteps(List<Map<String, String>> steps) {
    this.stepProperties = steps;
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder("Pipeline{");
    sb.append("name='").append(name).append('\'');
    sb.append(", steps={");
    stepProperties.forEach(step -> sb.append("[").append(step.toString()).append("]"));
    sb.append("}, pipelinesteps={");
    pipelineSteps.forEach(step -> sb.append("[").append(step.toString()).append("]"));
    sb.append("}}");
    return sb.toString();
  }
}
