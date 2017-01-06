package cl.bcs.risk.pipeline;

import cl.bcs.risk.RiskEtl;
import cl.bcs.risk.StepRegister;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * @author Alberto Hormazabal Cespedes
 * @author exaTech Ingenieria SpA. (info@exatech.cl)
 */
public class Pipeline {

  private String name;

  private String datasource;

  private List<Map<String, String>> stepProperties;

  private List<Step> pipelineSteps;

  private RiskEtl context;

  private volatile boolean executed = false;

  public Pipeline() {
    this.pipelineSteps = new LinkedList<>();
  }

  public void initialize() throws Exception {

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

    System.out.println("Running pipeline: " + name);

    // Exec Begin
    Iterator<Step> iter = pipelineSteps.iterator();
    BeginStep beginStep = (BeginStep) iter.next();
    Stream<? extends Record> pipelineStream = beginStep.begin();

    do {
      Step nextStep = iter.next();

      if (iter.hasNext()) {
        // Apply Filters
        if (nextStep instanceof FilterStep) {
          pipelineStream = ((FilterStep) nextStep).filter(pipelineStream);
        } else {
          throw new IllegalStateException("Illegal step reached. Not a filter: " + nextStep.toString());
        }

      } else {
        // Exec Finish
        if (nextStep instanceof FinalStep) {
          ((FinalStep) nextStep).finish(pipelineStream);
        } else {
          throw new IllegalStateException("Illegal step reached. Not a filter: " + nextStep.toString());
        }
      }
    }
    while (iter.hasNext());

    // Finish exec. Close stream
    pipelineStream.close();

  }


  /**
   * Validates the pipeline structure.
   *
   * @return
   */
  public boolean validate() {
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
          throw new IllegalStateException(name + ": Invalid pipeline: No-Filter Step found at the middle.");
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


  public void setContext(RiskEtl context) {
    this.context = context;
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

  public void setDatasource(String datasource) {
    this.datasource = datasource;
  }

  public String getDatasource() {
    return datasource;
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
    sb.append(", datasource='").append(datasource).append('\'');
    sb.append(", steps={");
    stepProperties.forEach(step -> sb.append("[").append(step.toString()).append("]"));
    sb.append("}, pipelinesteps={");
    pipelineSteps.forEach(step -> sb.append("[").append(step.toString()).append("]"));
    sb.append("}}");
    return sb.toString();
  }
}
