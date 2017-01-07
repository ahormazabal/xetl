package cl.bcs.risk.pipeline;

import cl.bcs.risk.utils.MutableRecord;

import java.util.Collection;
import java.util.Map.Entry;

/**
 * Registro que representa en proceso de una tupla en la operacion del ETL.
 *
 * Un pipeline procesa estos registros a medida que avanza para producir un resultado.
 *
 * @author Alberto Hormazabal Cespedes
 * @author exaTech Ingenieria SpA. (info@exatech.cl)
 */
public interface Record
    extends Iterable<String> {

  /**
   * @return Number of elements on this record.
   */
  int size();

  /**
   * Get the value at index index.
   * @param index
   * @return
   */
  String get(int index);

  /**
   * Get the value mapped at key.
   * @param key
   * @return
   */
  String get(String key);

  /**
   * Gets an unmodifiable collection of all entries (key,value).
   * @return
   */
  Collection<Entry<String, String>> entries();

  /**
   * Gets an numodifiable collections of all keys.
   * @return
   */
  Collection<String> keys();

  /**
   * Gets an unmodifiable collection of all values.
   * @return
   */
  Collection<String> values();

  /**
   * Crea una version mutable de este registro.
   *<p>
   * If the record is already mutable, the method will return the
   * same object.
   *</p>
   * @see MutableRecord
   * @return a {@link MutableRecord} version of this record.
   */
  static MutableRecord mutable(Record record) {
    if (record instanceof MutableRecord) {
      return (MutableRecord) record;
    } else {
      return new MutableRecord(record);
    }
  }

}
