package cl.bcs.risk.utils;

import cl.bcs.risk.pipeline.Record;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Utilidades para generar nemotecnicos IIF.
 *
 * Todos los metodos que hagan mencion a {@link cl.bcs.risk.pipeline.Record} asumen
 * que el registro es compatible con los campos definidos en el archivo O (operaciones)
 *
 *
 * @author Alberto Hormazabal Cespedes
 * @author exaTech Ingenieria SpA. (info@exatech.cl)
 */
public class IIFSymbolUtils {


  /**
   *
   * Genera el nemo IF de tipo DEP a partir de un {@link Record}.
   *
   *
   * @param r Record
   * @return DEP nemo
   */
  public static List<String> genDEPSymbols(Record r) throws ParseException {

    String moneda = r.get("moneda");
    String riesgo = r.get("riesgo");
    String plazo = r.get("plazo");
    String fecha = r.get("fecha");
    Set<String> risks = getRisks(riesgo);

    List<String> symbols = new ArrayList<>(risks.size());
    for(String rk: risks) {

//      DEP_[MONEDA]_[RIESGO][yyyymmdd]
      StringBuilder sb = new StringBuilder("DEP_");
      sb.append(moneda.trim()).append("_");
      sb.append(rk);
      sb.append(dateAdd(fecha, Integer.valueOf(plazo)));

      symbols.add(sb.toString());
    }
    return symbols;
  }

  public static Set<String> getRisks(String riskString) {
    String[] risks = riskString.split(";");
    HashSet<String> riskList = new HashSet<>(10);

    for(String r: risks) {
      riskList.add(r.trim());
    }

    return riskList;
  }


  /**
   * Add days to a date in YYYYMMDD format.
   *
   * @param startdate
   * @return
   */
  public static String dateAdd(String startdate, int days) throws ParseException {

    SimpleDateFormat sdf1 = new SimpleDateFormat("yyyy-MM-dd");
    SimpleDateFormat sdf2 = new SimpleDateFormat("yyyyMMdd");
    Calendar c = Calendar.getInstance();
    c.setTime(sdf1.parse(startdate));
    c.add(Calendar.DATE, days);  // number of days to add
    return sdf2.format(c.getTime());
  }

}
