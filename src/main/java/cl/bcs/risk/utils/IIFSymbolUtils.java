package cl.bcs.risk.utils;

import cl.bcs.risk.pipeline.Record;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Utilidades para generar nemotecnicos IIF.
 *
 * Todos los metodos que hagan mencion a {@link cl.bcs.risk.pipeline.Record} asumen
 * que el registro es compatible con los campos definidos en el archivo O (operaciones)
 *
 * @author Alberto Hormazabal Cespedes
 * @author exaTech Ingenieria SpA. (info@exatech.cl)
 */
public class IIFSymbolUtils {

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
   * Genera el nemo IF de tipo DEP a partir de un {@link Record}.
   *
   * @param r Record
   * @return DEP nemo
   */
  public static List<String> genDEPSymbols(Record r) throws ParseException {

    String moneda = r.get("moneda");
    String riesgo = r.get("riesgo");
    String plazo = r.get("plazo");
    String fecha = r.get("fecha");
    Set<String> risks = getRiskClassifications(riesgo);

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
   * @param record Registro compatible con archivo O
   * @param issuer Emisor del instrumento.
   * @return Nemo SVS.
   * @throws ParseException En caso de drama.
   */
  public static final String getSVSSymbol(Record record, Issuer issuer) throws ParseException {

    String plazo = record.get("plazo");
    String fecha = record.get("fecha");

    String issuerType = issuer.tip_ent;
    IIFAdjustment adjustment = IIFAdjustment.getAdjustment(record, issuer);
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
    final String svsInstrument = issuerType + adjustment.getCode() + issuerStr + dueDate;
    return svsInstrument;
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
