package cl.bcs.risk.steps.filters;

import cl.bcs.risk.DataSourceManager;
import cl.bcs.risk.pipeline.AbstractBaseStep;
import cl.bcs.risk.pipeline.FilterStep;
import cl.bcs.risk.pipeline.Pipeline;
import cl.bcs.risk.pipeline.Record;
import org.apache.commons.dbutils.QueryRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.math.MathContext;
import java.math.RoundingMode;
import java.sql.Connection;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

/**
 * Este filtro procesa registros de operaciones y descuenta la cantidad vendida
 * segun la cuenta de cobertura encontrada en la tabla de garantias.
 *
 * @author Alberto Hormazabal Cespedes
 * @author exaTech Ingenieria SpA. (info@exatech.cl)
 */
public class GCoverageDiscountFilter extends AbstractBaseStep
    implements FilterStep {

  private static final Logger LOG = LoggerFactory.getLogger(GCoverageDiscountFilter.class);

  // Config Parameters
  private String datasource;
  private String date;
  private String intraday;

  // Work data
  private Map<String, Coverage> coverages;


  @Override
  public String getType() {
    return "gCoverageDiscountFilter";
  }


  @Override
  public void initialize(Pipeline pipeline, Map<String, String> properties) throws Exception {
    super.initialize(pipeline, properties);

    date = getRequiredProperty("date");
    intraday = getRequiredProperty("intraday");
    datasource = getOptionalProperty("datasource", DataSourceManager.DEFAULT_DATASOURCE);

    Connection sqlCnx = getPipeline().getContext().getDataSourceManager().getConnection(datasource);
    QueryRunner qr = new QueryRunner();

    // Obtener cuentas de cobertura.
    coverages = qr.query(sqlCnx,
        "SELECT participante, instrumento, grupo, familia, moneda, cantidad FROM garantias WHERE fecha = cast(? AS DATE) AND intradiario = ? AND cuenta = 'COBERTURA'",
        rs -> {
          Map<String, Coverage> m = new HashMap<>(1000);
          while (rs.next()) {

            Coverage newCov = new Coverage();
            newCov.participante = rs.getString("participante");
            newCov.instrumento = rs.getString("instrumento");
            newCov.grupo = rs.getString("grupo");
            newCov.familia = rs.getString("familia");
            newCov.moneda = rs.getString("moneda");
            newCov.cantidad = rs.getBigDecimal("cantidad");
            m.put(newCov.getKey(), newCov);
          }
          return m;
        },
        date,
        intraday);

  }

  @Override
  public Stream<? extends Record> filter(Stream<? extends Record> recordStream) {
    LOG.info("Applying guarantee coverage discount filter");
    return recordStream
        .map(Record::mutable)
        .map(record -> {
          try {
            String chamber = record.get("camara");
            String participante = record.get("participante");
            String cliente = record.get("cliente");

            if (("RV".equals(chamber) || "PM".equals(chamber))
                && (participante != null && !participante.isEmpty())
                && (participante.equals(cliente))) {

              String instrumento = record.get("instrumento");
              String coverageKey = genKey(participante, instrumento);
              Coverage recordCoverage = coverages.get(coverageKey);

              if (recordCoverage != null) {
                // Calculate discounts.

                BigDecimal sellQty = new BigDecimal(record.get("cant_vendida"));
                BigDecimal sellAmnt = new BigDecimal(record.get("monto_vendido"));
                BigDecimal discountQty = recordCoverage.cantidad;

                BigDecimal newSellQty = sellQty.subtract(discountQty).max(BigDecimal.ZERO);
                BigDecimal newSellAmnt = sellAmnt.divide(sellQty, MathContext.DECIMAL32).multiply(newSellQty).setScale(0, RoundingMode.HALF_UP);
                BigDecimal newDiscount = discountQty.subtract(sellQty).max(BigDecimal.ZERO);

                // Update data
                record.set("cant_vendida", newSellQty.toPlainString());
                record.set("monto_vendido", newSellAmnt.toPlainString());
                recordCoverage.cantidad = newDiscount;
                coverages.put(coverageKey, recordCoverage);
                LOG.info(String.format(
                    "%s: Replaced Qty: [%s] to [%s].",
                    coverageKey,
                    sellQty.toPlainString(),
                    newSellQty.toPlainString()
                ));
              }
            }

            return record;

          } catch (Exception e) {
            throw new RuntimeException("Error processing coverage discount filter for record: " + record.toString(), e);
          }
        });

  }

  /**
   * Representa los datos de una cuenta de cobertura.
   */
  private class Coverage {
    String     participante;
    String     instrumento;
    String     grupo;
    String     familia;
    String     moneda;
    BigDecimal cantidad;


    /**
     * Obtiene la llave que representa la cuenta.
     *
     * @return
     */
    public String getKey() {
      return genKey(participante, instrumento);
    }

  }

  /**
   * Genera la llave de la cuenta de cobertura.
   */
  private static String genKey(String participante, String instrumento) {
    return Integer.valueOf(participante) + "-" + instrumento;
  }

}
