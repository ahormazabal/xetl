package cl.bcs.risk.steps;

import cl.bcs.risk.DataSourceManager;
import cl.bcs.risk.pipeline.AbstractBaseStep;
import cl.bcs.risk.pipeline.FilterStep;
import cl.bcs.risk.pipeline.Pipeline;
import cl.bcs.risk.pipeline.Record;
import cl.bcs.risk.utils.IIFSymbolUtils;
import cl.bcs.risk.utils.MutableRecord;
import org.apache.commons.dbutils.QueryRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

/**
 * Filtro para realizar conversiones durante la extraccion de operaciones.
 * Por ejemplo cambio de nemo IIF.
 *
 * @author Alberto Hormazabal Cespedes
 * @author exaTech Ingenieria SpA. (info@exatech.cl)
 */
public class OperationsFilter extends AbstractBaseStep
    implements FilterStep {

  private static final Logger LOG = LoggerFactory.getLogger(OperationsFilter.class);

  // Config Parameters
  private String datasource;
  private String date;


  // Work variables
  private Map<String, String> iifNemos;

  @Override
  public String getType() {
    return "operationsFilter";
  }


  @Override
  public void initialize(Pipeline pipeline, Map<String, String> properties) throws Exception {
    super.initialize(pipeline, properties);

    date = getRequiredProperty("date");
    datasource = getOptionalProperty("datasource", DataSourceManager.DEFAULT_DATASOURCE);


    Connection sqlCnx = getPipeline().getContext().getDataSourceManager().getConnection(datasource);
    QueryRunner qr = new QueryRunner();


    // extract IIF NEMOS from instrument parameters.
    iifNemos = qr.query(sqlCnx,
        "SELECT DISTINCT nemo AS nemo FROM parametros WHERE fecha = cast(? AS DATE) AND mercado = 'IF'",
        rs -> {
          Map<String, String> m = new HashMap<String, String>(20000);
          while (rs.next()) {
            String nemo = rs.getString("nemo");
            m.put(nemo, nemo);
          }
          return m;
        },
        date);


    // extract Gs to discount

  }

  @Override
  public Stream<? extends Record> filter(Stream<? extends Record> recordStream) {
    LOG.info("Applying operations filter");
    return recordStream
//        .map(Record::mutable)
        .flatMap(r -> {
          if (!"IF".equals(r.get("grupo"))) {
            return Stream.of(r);
          }

          try {
            // Generate record DEP nemos
            List<String> newnemos = IIFSymbolUtils.genDEPSymbols(r);

            // Add record SVS nemo

            // Start generation of new records.
            List<Record> newRecords = new ArrayList<Record>(10);
            newRecords.add(r);
            for (String newsym : newnemos) {
              if (iifNemos.containsKey(newsym)) {
                MutableRecord nr = new MutableRecord(r);
                nr.set("instrumento", newsym);
                newRecords.add(nr);
              }
            }

            return newRecords.stream();
          } catch (Exception e) {
            throw new RuntimeException("Error processing operations filter", e);
          }
        });
  }
}
