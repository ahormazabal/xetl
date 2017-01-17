package cl.bcs.risk.utils;

import java.text.DateFormat;
import java.text.ParseException;
import java.util.Calendar;

/**
 * @author Alberto Hormazabal Cespedes
 * @author exaTech Ingenieria SpA. (info@exatech.cl)
 */
public class DateUtils {

  /**
   * Convierte fechas entre formatos.
   * @param dateIn
   * @param sdfIn
   * @param sdfOut
   * @return
   * @throws ParseException
   */
  public static String formatDate(String dateIn, DateFormat sdfIn, DateFormat sdfOut) throws ParseException {
    Calendar c = Calendar.getInstance();
    c.setTime(sdfIn.parse(dateIn));
    return sdfOut.format(c.getTime());
  }

}
