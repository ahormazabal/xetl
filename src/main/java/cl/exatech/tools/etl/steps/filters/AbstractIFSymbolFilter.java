package cl.exatech.tools.etl.steps.filters;

import cl.exatech.tools.etl.DataSourceManager;
import cl.exatech.tools.etl.pipeline.AbstractBaseStep;
import cl.exatech.tools.etl.pipeline.FilterStep;
import cl.exatech.tools.etl.pipeline.Pipeline;
import cl.exatech.tools.etl.utils.IIFSymbolUtils;
import cl.exatech.tools.etl.utils.MutableRecord;
import org.apache.commons.dbutils.QueryRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.text.ParseException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Filtro base para las conversiones de nemos de IIF.
 *
 * @author Alberto Hormazabal Cespedes
 * @author exaTech Ingenieria SpA. (info@exatech.cl)
 */
public abstract class AbstractIFSymbolFilter extends AbstractBaseStep
    implements FilterStep {

  private static final Logger LOG = LoggerFactory.getLogger(AbstractIFSymbolFilter.class);

  // Config Parameters
  private String datasource;
  private String date;

  // Work data

  /** Mapa de nemos IF presentes en los parametros de instrumento (RiskAmerica) */
  private Map<String, String> ifNemos;

  /** Mapa de emisores-codigo svs */
  private Map<String, IIFSymbolUtils.Issuer> issuers;

  private Map<String, IIFSymbolUtils.Issuer> issuersBySvsCode;


  @Override
  public void initialize(Pipeline pipeline, Map<String, String> properties) throws Exception {
    super.initialize(pipeline, properties);

    LOG.info("Initializing Abstract IF Symbol Filter from: " + this.getClass().getSimpleName());

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
          Map<String, IIFSymbolUtils.Issuer> m = new HashMap<>(500);
          while (rs.next()) {
            IIFSymbolUtils.Issuer newIssuer = new IIFSymbolUtils.Issuer();
            newIssuer.emisor = rs.getString("emisor");
            newIssuer.cod_svs = rs.getString("cod_svs");
            newIssuer.tip_ent = rs.getString("tip_ent");
            m.put(newIssuer.emisor, newIssuer);
          }
          return m;
        });

    // Index issuers by svs code.
    issuersBySvsCode = new HashMap<>(issuers.size());
    issuers.values().forEach(issuer -> {
      issuersBySvsCode.put(issuer.cod_svs, issuer);
    });

  }


  protected String getDate() {
    return date;
  }

  protected String getDatasource() {
    return datasource;
  }

  protected Map<String, IIFSymbolUtils.Issuer> getIssuers() {
    return issuers;
  }

  public Map<String, IIFSymbolUtils.Issuer> getIssuersBySvsCode() {
    return issuersBySvsCode;
  }

  protected Map<String, String> getIfNemos() {
    return ifNemos;
  }

  protected MutableRecord performIFSymbolReplacement(
      MutableRecord record,
      IIFSymbolUtils.Issuer issuer,
      String moneda,
      String riesgos,
      String fecha,
      String plazo) throws ParseException {


    // Generar Nemo SVS
    if (issuer != null) {
      String svsSymbol = null;
      svsSymbol = IIFSymbolUtils.getSVSSymbol(issuer, moneda, fecha, plazo);
      if (isValidSymbol(svsSymbol)) {
        return replaceRecordSymbol(record, svsSymbol);
      }
    }

    // No svs, Generate record DEP nemos
    List<String> depSymbols = IIFSymbolUtils.genDEPSymbols(moneda, riesgos, fecha, plazo);
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
  }

  private MutableRecord replaceRecordSymbol(MutableRecord record, String newSymbol) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Switching Symbol <" + record.get("instrumento") + "> to <" + newSymbol + ">");
    }
    record.set("instrumento", newSymbol);
    return record;
  }


  protected boolean isValidSymbol(String symbol) {
    return symbol != null && !symbol.isEmpty() && ifNemos.containsKey(symbol);
  }

}
