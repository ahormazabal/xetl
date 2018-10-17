package cl.exatech.tools.etl.steps.filters;

import cl.exatech.tools.etl.pipeline.FilterStep;
import cl.exatech.tools.etl.pipeline.Pipeline;
import cl.exatech.tools.etl.pipeline.Record;
import cl.exatech.tools.etl.utils.IIFSymbolUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.stream.Stream;

/**
 * Este filtro procesa registros de operaciones y modifica el nemo cuando
 * corresponde a un registro IF.
 *
 * @author Alberto Hormazabal Cespedes
 * @author exaTech Ingenieria SpA. (info@exatech.cl)
 */
public class OperationsIFSymbolFilter extends AbstractIFSymbolFilter
    implements FilterStep {

  private static final Logger LOG = LoggerFactory.getLogger(OperationsIFSymbolFilter.class);


  @Override
  public String getType() {
    return "operationsIFSymbolFilter";
  }


  @Override
  public void initialize(Pipeline pipeline, Map<String, String> properties) throws Exception {
    super.initialize(pipeline, properties);


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
            // Obtener Datos del registro.
            String                plazo   = record.get("plazo");
            String                fecha   = record.get("fecha");
            String                moneda  = record.get("moneda");
            String                riesgos = record.get("riesgo");
            IIFSymbolUtils.Issuer issuer  = getIssuer(record);

            return performIFSymbolReplacement(record, issuer, moneda, riesgos, fecha, plazo);
          } catch (Exception e) {
            throw new RuntimeException("Error processing operations filter for record: " + record.toString(), e);
          }
        });
  }

  /**
   * Determina el emisor del instrumento representado en el {@link Record}.
   *
   * @param record
   * @return
   */
  protected IIFSymbolUtils.Issuer getIssuer(Record record) {

    IIFSymbolUtils.Issuer issuer = getIssuers().get(record.get("emisor"));
    if (issuer == null || "CENTRAL".equals(issuer.emisor)) { // Caso especial PDBC PRBC
      String instrumento = record.get("instrumento");
      switch (instrumento) {
        case "PDBC":
        case "PRBC":
          issuer = new IIFSymbolUtils.Issuer();
          issuer.emisor = "CENTRAL";
          issuer.cod_svs = instrumento.substring(0, 4);
          issuer.tip_ent = "B";
          break;
      }
    }

    return issuer;
  }


}
