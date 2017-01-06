package cl.bcs.risk;

import cl.bcs.risk.pipeline.Pipeline;
import cl.bcs.risk.pipeline.Step;
import cl.bcs.risk.steps.FixDecimalsFilter;
import cl.bcs.risk.steps.LoadCsv;
import cl.bcs.risk.steps.SaveDB;

import java.util.HashMap;
import java.util.Map;

/**
 * @author Alberto Hormazabal Cespedes
 * @author exaTech Ingenieria SpA. (info@exatech.cl)
 */
public final class StepRegister {

  private static final Map<String, Class<? extends Step>> register = new HashMap<>();

  static {

    // Register Begin Steps
    register.put("loadcsv", LoadCsv.class);

    // Register Fiters
    register.put("fixdecimals", FixDecimalsFilter.class);

    // Register Finalizers
    register.put("savedb", SaveDB.class);
  }


  public static Step initStep(String stepType, Map<String, String> stepProps, Pipeline stepPipeline) throws Exception {
    Class<? extends Step> clazz = register.get(stepType);
    if (clazz == null) {
      throw new IllegalArgumentException("Step type not found. Check config and register: " + stepType);
    }

    Step newStep = clazz.newInstance();
    newStep.initialize(stepPipeline, stepProps);
    return newStep;
  }


}
