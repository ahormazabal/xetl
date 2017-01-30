package cl.bcs.risk.steps.filters;

import cl.bcs.risk.DataSourceManager;
import cl.bcs.risk.pipeline.AbstractBaseStep;
import cl.bcs.risk.pipeline.FilterStep;
import cl.bcs.risk.pipeline.Pipeline;
import cl.bcs.risk.pipeline.Record;
import cl.bcs.risk.utils.IIFSymbolUtils;
import cl.bcs.risk.utils.IIFSymbolUtils.Issuer;
import cl.bcs.risk.utils.MutableRecord;
import org.apache.commons.dbutils.QueryRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

/**
 * Este filtro procesa registros de operaciones y agrega registros adicionales cuando
 * corresponde a un registro IF. Basicamente agrega un registro adicional con nemo DEP y otro
 * con nemo SVS, siempre y cuando estos nemos nuevos esten presentes en los parametros de instrumento
 * del dia de proceso.
 *
 * @author Alberto Hormazabal Cespedes
 * @author exaTech Ingenieria SpA. (info@exatech.cl)
 */
public class OperationsIFSymbolFilter extends AbstractBaseStep
    implements FilterStep {

  private static final Logger LOG = LoggerFactory.getLogger(OperationsIFSymbolFilter.class);

  // Config Parameters
  private String datasource;
  private String date;

  // Work data

  /** Mapa de nemos IF presentes en los parametros de instrumento (RiskAmerica) */
  private Map<String, String> ifNemos;

  /** Mapa de emisores-codigo svs */
  private Map<String, Issuer> issuers;

  @Override
  public String getType() {
    return "operationsIFSymbolFilter";
  }


  @Override
  public void initialize(Pipeline pipeline, Map<String, String> properties) throws Exception {
    super.initialize(pipeline, properties);

    date = getRequiredProperty("date");
    datasource = getOptionalProperty("datasource", DataSourceManager.DEFAULT_DATASOURCE);


    Connection sqlCnx = getPipeline().getContext().getDataSourceManager().getConnection(datasource);
    QueryRunner qr = new QueryRunner();


    // extract IIF NEMOS to check instrument parameters.
    ifNemos = qr.query(sqlCnx,
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

    // Extract IIF Issuers
    issuers = qr.query(sqlCnx,
        "SELECT emisor, cod_svs, tip_ent FROM emisores WHERE ind_iif = 'S'",
        rs -> {
          Map<String, Issuer> m = new HashMap<>(500);
          while (rs.next()) {
            Issuer newIssuer = new Issuer();
            newIssuer.emisor = rs.getString("emisor");
            newIssuer.cod_svs = rs.getString("cod_svs");
            newIssuer.tip_ent = rs.getString("tip_ent");
            m.put(newIssuer.emisor, newIssuer);
          }
          return m;
        });
  }

  @Override
  public Stream<? extends Record> filter(Stream<? extends Record> recordStream) {
    LOG.info("Applying operations filter");
    return recordStream
        .map(Record::mutable)
        .map(record -> {

          if (!"IF".equals(record.get("grupo"))) {
            return record;
          }

          try {

            // Generar Nemo SVS
            String svsSymbol = null;
            Issuer issuer = issuers.get(record.get("emisor"));
            if (issuer == null) { // Caso especial PDBC PRBC
              String instrumento = record.get("instrumento");
              switch (instrumento) {
                case "PDBC":
                case "PRBC":
                  issuer = new Issuer();
                  issuer.emisor = "CENTRAL";
                  issuer.cod_svs = instrumento;
                  issuer.tip_ent = "B";
                  break;
              }
            }

            if (issuer != null) {
              svsSymbol = IIFSymbolUtils.getSVSSymbol(record, issuer);
              if (isValidSymbol(svsSymbol)) {
                return replaceRecordSymbol(record, svsSymbol);
              }
            }

            // No svs, Generate record DEP nemos
            List<String> depSymbols = IIFSymbolUtils.genDEPSymbols(record);
            if (depSymbols != null && depSymbols.size() > 0) {

              // Reverse look for the first valid one.
              for (int i = (depSymbols.size() - 1); i >= 0; i--) {
                if (isValidSymbol(depSymbols.get(i))) {
                  return replaceRecordSymbol(record, depSymbols.get(i));
                }
              }
            }

            if (LOG.isDebugEnabled()) {
              LOG.debug("No match for record: " + record.toString());
            }

            // No match return original.
            return record;
          } catch (Exception e) {
            throw new RuntimeException("Error processing operations filter for record: " + record.toString(), e);
          }
        });

  }

  private MutableRecord replaceRecordSymbol(MutableRecord record, String newSymbol) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Switching Symbol <" + record.get("instrumento") + "> to <" + newSymbol + ">");
    }
    record.set("instrumento", newSymbol);
    return record;
  }


  private boolean isValidSymbol(String symbol) {
    return symbol != null && !symbol.isEmpty() && ifNemos.containsKey(symbol);
  }

}
