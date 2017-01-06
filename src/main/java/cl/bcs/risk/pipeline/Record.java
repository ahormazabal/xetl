package cl.bcs.risk.pipeline;

import cl.bcs.risk.utils.MutableOrderedRecord;

import java.util.Collection;
import java.util.Map.Entry;
import java.util.Set;

/**
 * @author Alberto Hormazabal Cespedes
 * @author exaTech Ingenieria SpA. (info@exatech.cl)
 */
public interface Record
    extends Iterable<String> {

  String get(int i);

  String get(String key);

  Set<String> keys();

  Collection<String> values();

  Set<Entry<String, String>> entrySet();

  int numFields();

  /**
   * Crea una version mutable de este registro.
   * If the record is already mutable, the method will return the
   * same object.
   *
   * @return a {@link MutableOrderedRecord} version of this record.
   */
  static MutableOrderedRecord mutable(Record record) {
    if (record instanceof MutableOrderedRecord) {
      return (MutableOrderedRecord) record;
    } else {
      return new MutableOrderedRecord(record);
    }
  }

}
