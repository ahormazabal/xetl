package cl.bcs.risk.pipeline;

import cl.bcs.risk.utils.MutableRecord;

import java.util.Collection;
import java.util.Map.Entry;

/**
 * @author Alberto Hormazabal Cespedes
 * @author exaTech Ingenieria SpA. (info@exatech.cl)
 */
public interface Record
    extends Iterable<String> {

  int size();

  String get(int index);

  String get(String key);

  Collection<Entry<String, String>> entries();

  Collection<String> keys();

  Collection<String> values();

  /**
   * Crea una version mutable de este registro.
   * If the record is already mutable, the method will return the
   * same object.
   *
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
