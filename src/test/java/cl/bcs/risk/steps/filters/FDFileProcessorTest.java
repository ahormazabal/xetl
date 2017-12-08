package cl.bcs.risk.steps.filters;

import cl.bcs.risk.pipeline.FilterStep;
import cl.bcs.risk.pipeline.Pipeline;
import cl.bcs.risk.pipeline.Record;
import cl.bcs.risk.utils.MutableRecord;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.*;

import static cl.bcs.risk.steps.filters.FDFileProcessor.*;

/**
 * @author Alberto Hormazabal Cespedes
 * @author exaTech Ingenieria SpA. (info@exatech.cl)
 */
public class FDFileProcessorTest {

  private static final Logger LOG = LoggerFactory.getLogger(FDFileProcessorTest.class);

  private static final double PRECISION           = 0.000001;
  private static final String DATA_FILE           = "/FD_Generation_Test_Data.csv";
  private static final char   DATA_FILE_DELIMITER = ';';


  @BeforeClass
  public static void initTest() {
    LOG.info("FD File Processor Tests...");
  }

  @Test
  public void scenariosValueTest() {
    LOG.info("Begin Scenarios Value Test");

    double[] evalues = {
        -5534599.11,
        3458942.94,
        -3100799.30,
        165086.77,
        -1125782.66,
        -26268489.74,
        26680166.56,
        -1252944.97,
        3754309.20,
        4085738.10,
        -427432.17,
        -4279963.48,
        -2474250.09,
        -13466186.88,
        965022134.33,
        -942315829.30,
        -1728122.36,
        -1191977.82,
    };

    double result = FDFileProcessor.scenarioValue(0,
        DoubleStream.of(evalues)
        .mapToObj(e -> new FDFileProcessor.Entry("PT", "RV", new Double[]{e}))
        .collect(Collectors.toList()));

    Assert.assertEquals(991702300.89, result, PRECISION);
  }


  @Test
  public void generationTest() throws Exception {
    LOG.info("Begin Scenarios Data Generation Test");

    FilterStep generationFilter = new FDFileProcessor();
    generationFilter.initialize(new Pipeline(), new HashMap<>());

    // Load test data.
    CSVParser csvParser = new CSVParser(
        new InputStreamReader(this.getClass().getResourceAsStream(DATA_FILE)),
        CSVFormat.newFormat(DATA_FILE_DELIMITER)
    );

    Stream<Record> stream = StreamSupport.stream(csvParser.spliterator(), false)
        .map(line -> {
          String pt = line.get(0).trim();
          String evaluesString = IntStream.range(1, line.size())
              .mapToObj(line::get)
              .map(String::trim)
              .collect(Collectors.joining(",", "[", "]"));
          return new MutableRecord()
              .append("fecha_proceso", "1979-11-20")
              .append(LEVEL_FIELD, "PT")
              .append(CM_FIELD, "RV")
              .append(PT_FIELD, pt)
              .append("cl", "")
              .append("ct", "")
              .append("f", "")
              .append("riesgot", "")
              .append(SCENARIOS_FIELD, evaluesString);
        });

    // Build chain
    Stream<? extends Record> testStream = generationFilter.filter(stream);

    Map<String, Double> results = testStream
        .collect(Collectors.toMap(r -> r.get(PT_FIELD), r -> Double.valueOf(r.get(RIESGO_TT_KEY))));

    // Test Results
    final double precision = 0.00001;
    Assert.assertEquals(11150269.84, results.get("20"), precision);
    Assert.assertEquals(-41229362.36, results.get("35"), precision);
    Assert.assertEquals(13437693.32, results.get("43"), precision);
    Assert.assertEquals(-1545426.05, results.get("48"), precision);
    Assert.assertEquals(-7719568.46, results.get("51"), precision);
    Assert.assertEquals(-157491887.38, results.get("54"), precision);
    Assert.assertEquals(120875191.48, results.get("58"), precision);
    Assert.assertEquals(-1548210.43, results.get("59"), precision);
    Assert.assertEquals(-21653137.83, results.get("62"), precision);
    Assert.assertEquals(57499793.62, results.get("66"), precision);
    Assert.assertEquals(-7936886.75, results.get("69"), precision);
    Assert.assertEquals(-10261466.48, results.get("70"), precision);
    Assert.assertEquals(-67018668.52, results.get("82"), precision);
    Assert.assertEquals(-372192503.80, results.get("85"), precision);
    Assert.assertEquals(-3940406892.05, results.get("86"), precision);
    Assert.assertEquals(4414295064.41, results.get("88"), precision);
    Assert.assertEquals(7675977.56, results.get("90"), precision);
    Assert.assertEquals(4070019.86, results.get("293"), precision);

  }


}
