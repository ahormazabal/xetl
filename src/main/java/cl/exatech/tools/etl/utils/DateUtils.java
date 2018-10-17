package cl.exatech.tools.etl.utils;

import java.text.DateFormat;
import java.text.ParseException;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
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

  /**
   * Calcula el numero de dias entre dos fechas yyyy-mm-dd.
   *
   * @param startdate
   * @param endDate
   * @return
   */
  public static long daysBetween(String startdate, String endDate) {
    LocalDate sd = LocalDate.parse(startdate, DateTimeFormatter.ofPattern("yyyy-MM-dd"));
    LocalDate ed = LocalDate.parse(endDate, DateTimeFormatter.ofPattern("yyyy-MM-dd"));
    return sd.until(ed, ChronoUnit.DAYS);
  }


}
