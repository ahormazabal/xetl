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
import java.util.ArrayList;
import java.util.Arrays;
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

  private static final double PRECISION           = 0.00000000001;
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

    // Load Test Data.
    Stream<Record> dataStream = StreamSupport.stream(csvParser.spliterator(), false)
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

    // Add empty Data cases.
    Record emptyTemplate = new MutableRecord()
        .append("fecha_proceso", "1979-11-20")
        .append(LEVEL_FIELD, "CL")
        .append(CM_FIELD, "RF")
        .append(PT_FIELD, "35")
        .append("cl", "")
        .append("ct", "")
        .append("f", "")
        .append("riesgot", "")
        .append(SCENARIOS_FIELD, "[]");
    Stream<Record> emptyData = Stream.of(
        new MutableRecord(emptyTemplate).set(PT_FIELD, "35"),
        new MutableRecord(emptyTemplate).set(PT_FIELD, "20"),
        new MutableRecord(emptyTemplate).set(PT_FIELD, "86"),
        new MutableRecord(emptyTemplate).set(PT_FIELD, "90"),
        new MutableRecord(emptyTemplate).set(PT_FIELD, "92"),
        new MutableRecord(emptyTemplate).set(PT_FIELD, "70"),
        new MutableRecord(emptyTemplate).set(PT_FIELD, "72"),
        new MutableRecord(emptyTemplate).set(PT_FIELD, "236")
    );

    // Build chain
    Stream<? extends Record> testStream = generationFilter
        .filter(Stream.concat(dataStream, emptyData));


    Map<String, Map<String, Double>> results = testStream
        .collect(Collectors.groupingBy(r -> r.get(LEVEL_FIELD) + "-" + r.get(CM_FIELD),
            Collectors.toMap(r -> r.get(PT_FIELD), r -> Double.valueOf(r.get(RIESGO_TT_KEY)))
            ));

//    Map<String, Double> results = testStream
//        .collect(Collectors.toMap(r -> r.get(PT_FIELD), r -> Double.valueOf(r.get(RIESGO_TT_KEY))));

    // Test Results
    LOG.info("Batch 1: Testing numeric results.");
    Map<String, Double> batch1Results = results.get("PT-RV");
    Assert.assertEquals(11150269.84, batch1Results.get("20"), PRECISION);
    Assert.assertEquals(-41229362.36, batch1Results.get("35"), PRECISION);
    Assert.assertEquals(13437693.32, batch1Results.get("43"), PRECISION);
    Assert.assertEquals(-1545426.05, batch1Results.get("48"), PRECISION);
    Assert.assertEquals(-7719568.46, batch1Results.get("51"), PRECISION);
    Assert.assertEquals(-157491887.38, batch1Results.get("54"), PRECISION);
    Assert.assertEquals(120875191.48, batch1Results.get("58"), PRECISION);
    Assert.assertEquals(-1548210.43, batch1Results.get("59"), PRECISION);
    Assert.assertEquals(-21653137.83, batch1Results.get("62"), PRECISION);
    Assert.assertEquals(57499793.62, batch1Results.get("66"), PRECISION);
    Assert.assertEquals(-7936886.75, batch1Results.get("69"), PRECISION);
    Assert.assertEquals(-10261466.48, batch1Results.get("70"), PRECISION);
    Assert.assertEquals(-67018668.52, batch1Results.get("82"), PRECISION);
    Assert.assertEquals(-372192503.80, batch1Results.get("85"), PRECISION);
    Assert.assertEquals(-3940406892.05, batch1Results.get("86"), PRECISION);
    Assert.assertEquals(4414295064.41, batch1Results.get("88"), PRECISION);
    Assert.assertEquals(7675977.56, batch1Results.get("90"), PRECISION);
    Assert.assertEquals(4070019.86, batch1Results.get("293"), PRECISION);

    LOG.info("Batch 2: Testing zero-evalue results.");
    Map<String, Double> batch2Results = results.get("CL-RF");
    Assert.assertEquals(0.0, batch2Results.get("35"), 0);
    Assert.assertEquals(0.0, batch2Results.get("20"), 0);
    Assert.assertEquals(0.0, batch2Results.get("86"), 0);
    Assert.assertEquals(0.0, batch2Results.get("90"), 0);
    Assert.assertEquals(0.0, batch2Results.get("92"), 0);
    Assert.assertEquals(0.0, batch2Results.get("70"), 0);
    Assert.assertEquals(0.0, batch2Results.get("72"), 0);
    Assert.assertEquals(0.0, batch2Results.get("236"),0);

  }

}
