package cl.exatech.tools.etl.utils;

import cl.exatech.tools.etl.pipeline.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Utilidades para generar nemotecnicos IIF.
 *
 * Todos los metodos que hagan mencion a {@link Record} asumen
 * que el registro es compatible con los campos definidos en el archivo O (operaciones)
 *
 * @author Alberto Hormazabal Cespedes
 * @author exaTech Ingenieria SpA. (info@exatech.cl)
 */
public class IIFSymbolUtils {
  private static final Logger LOG = LoggerFactory.getLogger(IIFSymbolUtils.class);

  /** Formato Fecha SVS. */
  public static final DateFormat FORMAT_DATE_SVS = new SimpleDateFormat("ddMMyy");

  /** Formato fecha DEP */
  public static final DateFormat FORMAT_DATE_DEP = new SimpleDateFormat("yyyyMMdd");

//  /**
//   * Largo del Instrumento SVS.
//   */
//  private static final int        SVS_SYMBOL_LENGTH = 12;

  /** Valor Mínimo del Emisor. */
  private static final int MIN_ISSUER_LENGTH = 3;

  /** Valor Máximo del Emisor. */
  private static final int MAX_ISSUER_LENGTH = 4;

  /** String guión utilitario. */
  private static final String DASH = "-";


  /**
   * Parsea y separa un string de clasificacion de riesgo.
   *
   * @param riskString String de clasfificaion de riesgo segun archivo O.
   * @return Set con las clasificaciones separadas.
   */
  public static Set<String> getRiskClassifications(String riskString) {
    String[] risks = riskString.split(";");
    HashSet<String> riskList = new HashSet<>(10);
    for (String r : risks) {
      riskList.add(r.trim());
    }
    return riskList;
  }

    /**
   * Add days to a yyyy-mm-dd date and convert into specified format.
   *
   * @param startdate
   * @return
   */
  public static String dueDate(String startdate, int days, DateFormat format) throws ParseException {
    SimpleDateFormat sdf1 = new SimpleDateFormat("yyyy-MM-dd");
    Calendar c = Calendar.getInstance();
    c.setTime(sdf1.parse(startdate));
    c.add(Calendar.DATE, days);  // number of days to add
    return format.format(c.getTime());
  }


  /**
   * Genera el nemo IF de tipo DEP.
   *
   * @param moneda Moneda del IF
   * @param riesgos String con clasificaciones de riesgo del instrumento, separadas por ';'. EJ: "N1; N2"
   * @param fecha Fecha a partir de la cual calcular el vencimiento.
   * @param plazo Dias al vencimiento.
   * @return Lista con todos los nemos DEP generados.
   * @throws ParseException En caso de drama.
   */
  public static List<String> genDEPSymbols(String moneda, String riesgos, String fecha, String plazo) throws ParseException {

    if (moneda == null || riesgos == null || plazo == null || fecha == null) {
      LOG.warn(String.format(
          "Skipping DEP symbol generation for IF record with incomplete data: moneda<%s>, riesgos<%s>, fecha<%s>, plazo<%s>",
          moneda, riesgos, fecha, plazo));
      return Collections.emptyList();
    }


    Collection<String> risks = getRiskClassifications(riesgos);
    // Caso especial CLF
    if ("CLF".equals(moneda))
      risks.add("XX");


    List<String> symbols = new ArrayList<>(risks.size());
    for (String rk : risks) {

//      DEP_[MONEDA]_[RIESGO][yyyymmdd]
      StringBuilder sb = new StringBuilder("DEP_");
      sb.append(moneda.trim()).append("_");
      sb.append(rk);
      sb.append(dueDate(fecha, Integer.valueOf(plazo), FORMAT_DATE_DEP));

      symbols.add(sb.toString());
    }
    return symbols;
  }


  /**
   * Obtiene el nemo svs del registro y emisor indicados.
   *
   * @param issuer
   * @param moneda
   * @param fecha
   * @param plazo
   * @return
   * @throws ParseException
   */
  public static final String getSVSSymbol(Issuer issuer, String moneda, String fecha, String plazo) throws ParseException {

    String issuerType = issuer.tip_ent;
    IIFAdjustment adjustment = IIFAdjustment.getAdjustment(moneda, issuer);
    String dueDate = dueDate(fecha, Integer.valueOf(plazo), FORMAT_DATE_SVS);

    String issuerSvsCode = issuer.cod_svs;

    if (issuerType == null) {
      throw new NullPointerException("Issuer type cannot be null");
    }
    if (adjustment == null) {
      throw new NullPointerException("Currency cannot be null");
    }
    if (issuerSvsCode == null) {
      throw new NullPointerException("Issuer SVS code cannot be null");
    }
    if (dueDate == null) {
      throw new NullPointerException("Due date cannot be null");
    }
    String issuerStr;
    if (MIN_ISSUER_LENGTH == issuerSvsCode.length() || MAX_ISSUER_LENGTH == issuerSvsCode.length()) {
      if (MIN_ISSUER_LENGTH == issuerSvsCode.length()) {
        issuerStr = issuerSvsCode + DASH;
      } else {
        issuerStr = issuerSvsCode;
      }
    } else {
      return null;
    }
    return issuerType + adjustment.getCode() + issuerStr + dueDate;
  }

  /**
   * Clase para representar un emisor y su informacion.
   */
  public static class Issuer {
    public String emisor;
    public String cod_svs;
    public String tip_ent;
  }

}
