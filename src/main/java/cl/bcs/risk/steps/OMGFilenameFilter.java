package cl.bcs.risk.steps;

import cl.bcs.risk.pipeline.AbstractBaseStep;
import cl.bcs.risk.pipeline.FilterStep;
import cl.bcs.risk.pipeline.Pipeline;
import cl.bcs.risk.pipeline.Record;
import cl.bcs.risk.utils.DateUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.Map;
import java.util.stream.Stream;

/**
 * Conversor para archivos O, M y G.
 *
 * Agrega dos columnas al principio del registro, fecha e intradiario, a partir del nombre de
 * archivo entregado.
 *
 * @author Alberto Hormazabal Cespedes
 * @author exaTech Ingenieria SpA. (info@exatech.cl)
 */
public class OMGFilenameFilter extends AbstractBaseStep
    implements FilterStep {

  private static final Logger LOG = LoggerFactory.getLogger(OMGFilenameFilter.class);

  public static final String DATE_FIELD = "fecha";
  public static final String INTRADAY_FIELD = "intradiario";

  private String filename;

  SimpleDateFormat sdfDateOut = new SimpleDateFormat("yyyy-MM-dd");
  SimpleDateFormat sdfDateIn  = new SimpleDateFormat("yyMMdd");

  @Override
  public String getType() {
    return "omgFile";
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

            // add fields from filename
            String intradayId = filename.substring(1, 2);
            String filedate = filename.substring(2, 8);
            record.insert(0, DATE_FIELD, DateUtils.formatDate(filedate, sdfDateIn, sdfDateOut));
            record.insert(1, INTRADAY_FIELD, intradayId);

            return record;
          } catch (Exception e) {
            throw new RuntimeException("Error loading operations: " + e.getMessage(), e);
          }
        });
  }


}
