package cl.exatech.tools.etl;

import cl.exatech.tools.etl.pipeline.Pipeline;
import cl.exatech.tools.etl.pipeline.Step;
import cl.exatech.tools.etl.steps.begin.LoadCsv;
import cl.exatech.tools.etl.steps.begin.LoadDB;
import cl.exatech.tools.etl.steps.filters.*;
import cl.exatech.tools.etl.steps.finale.BatchInsert;
import cl.exatech.tools.etl.steps.finale.SaveCSV;
import cl.exatech.tools.etl.steps.finale.SaveDB;
import cl.exatech.tools.etl.steps.finale.StdoutFinalStep;

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
    register.put("pt_cl1_LevelCorrector", PT_CL1_LevelCorrector.class);
    register.put("format", ColumnFormatter.class);
    register.put("fdFileProcessor", FDFileProcessor.class);

    // Register Finalizers
    register.put("savedb", SaveDB.class);
    register.put("savecsv", SaveCSV.class);
    register.put("stdout", StdoutFinalStep.class);
    register.put("batchinsert", BatchInsert.class);
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
