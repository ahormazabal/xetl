package cl.bcs.risk;

import cl.bcs.risk.pipeline.Pipeline;
import cl.bcs.risk.pipeline.Step;
import cl.bcs.risk.steps.begin.LoadCsv;
import cl.bcs.risk.steps.begin.LoadDB;
import cl.bcs.risk.steps.filters.*;
import cl.bcs.risk.steps.finale.SaveCSV;
import cl.bcs.risk.steps.finale.SaveDB;
import cl.bcs.risk.steps.finale.StdoutFinalStep;

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
    register.put("loaddb", LoadDB.class);

    // Register Fiters
    register.put("fixdecimals", FixDecimalsFilter.class);
    register.put("dateformat", DateFormatFilter.class);
    register.put("operationsIFSymbolFilter", OperationsIFSymbolFilter.class);
    register.put("guaranteesIFSymbolFilter", GuaranteesIFSymbolFilter.class);
    register.put("omgFile", OMGFilenameFilter.class);
    register.put("zeroToRight", ZeroToRightFilter.class);
    register.put("mValuation", InstrumentMValuationFilter.class);
    register.put("removeOMG", RemoveOMGFilter.class);
    register.put("gCoverageDiscountFilter", GCoverageDiscountFilter.class);


    // Register Finalizers
    register.put("savedb", SaveDB.class);
    register.put("savecsv", SaveCSV.class);
    register.put("stdout", StdoutFinalStep.class);
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
