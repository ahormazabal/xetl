package cl.bcs.risk.steps;

import cl.bcs.risk.pipeline.AbstractBaseStep;
import cl.bcs.risk.pipeline.FilterStep;
import cl.bcs.risk.pipeline.Pipeline;
import cl.bcs.risk.pipeline.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Map;
import java.util.stream.Stream;

/**
 * @author Alberto Hormazabal Cespedes
 * @author exaTech Ingenieria SpA. (info@exatech.cl)
 */
public class OperationsLoaderFilter extends AbstractBaseStep
    implements FilterStep {

  private static final Logger LOG = LoggerFactory.getLogger(OperationsLoaderFilter.class);

  private String filename;


  SimpleDateFormat sdfDateOut = new SimpleDateFormat("yyyy-MM-dd");
  SimpleDateFormat sdfLiqIn   = new SimpleDateFormat("dd-MM-yyyy");
  SimpleDateFormat sdfDateIn  = new SimpleDateFormat("yyMMdd");


  @Override
  public String getType() {
    return "operationsLoader";
  }

  @Override
  public void initialize(Pipeline pipeline, Map<String, String> properties) throws Exception {
    super.initialize(pipeline, properties);

    filename = getRequiredProperty("filename");
  }

  @Override
  public Stream<? extends Record> filter(Stream<? extends Record> recordStream) {
    return recordStream
        .map(Record::mutable)
        .map(record -> {
          try {

            // add date field
            String filedate = filename.substring(2, 8);
            record.insert(0, "FECHA", formatDate(filedate, sdfDateIn, sdfDateOut));

            // reformat fecha liquidacion
            record.set("FECHA LIQUIDACION", formatDate(record.get("FECHA LIQUIDACION"), sdfLiqIn, sdfDateOut));


            return record;
          } catch (Exception e) {
            throw new RuntimeException("Error loading operations: " + e.getMessage(), e);
          }
        });
  }

  public static String formatDate(String dateIn, SimpleDateFormat sdfIn, SimpleDateFormat sdfOut) throws ParseException {
    Calendar c = Calendar.getInstance();
    c.setTime(sdfIn.parse(dateIn));
    return sdfOut.format(c.getTime());
  }

}
