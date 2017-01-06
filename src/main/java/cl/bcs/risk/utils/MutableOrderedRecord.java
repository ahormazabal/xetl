package cl.bcs.risk.utils;

import cl.bcs.risk.pipeline.Record;

import java.util.*;

/**
 * Registro mutable con conservacion del orden de columnas.
 *
 * @author Alberto Hormazabal Cespedes
 * @author exaTech Ingenieria SpA. (info@exatech.cl)
 */
public class MutableOrderedRecord
    implements Record {

  private final LinkedHashMap<String, String> fieldMap;
  private ArrayList<String> keyList;

  public MutableOrderedRecord() {
    this.fieldMap = new LinkedHashMap<>();
    reindex();
  }

  public MutableOrderedRecord(Map<String, String> fieldMap) {
    this.fieldMap = new LinkedHashMap<>(fieldMap);
    reindex();
  }

  public MutableOrderedRecord(Record record) {
      this.fieldMap = new LinkedHashMap<>();
      record.entrySet().forEach(entry -> {
        fieldMap.put(entry.getKey(), entry.getValue());
      });
      reindex();
  }

  private void reindex() {
    this.keyList = new ArrayList<>(fieldMap.keySet());
  }

  @Override
  public String get(int i) {
    return fieldMap.get(keyList.get(i));
  }

  public void set(int i, String value) {
    fieldMap.put(keyList.get(i), value);
    reindex();
  }

  @Override
  public String get(String key) {
    return fieldMap.get(key);
  }

  public void set(String key, String value) {
    fieldMap.put(key, value);
    reindex();
  }

  @Override
  public Set<String> keys() {
    return Collections.unmodifiableSet(fieldMap.keySet());
  }

  @Override
  public Collection<String> values() {
    return Collections.unmodifiableCollection(fieldMap.values());
  }

  @Override
  public Set<Map.Entry<String, String>> entrySet() {
    return Collections.unmodifiableSet(fieldMap.entrySet());
  }

  @Override
  public int numFields() {
    return fieldMap.size();
  }

  @Override
  public Iterator<String> iterator() {
    return fieldMap.values().iterator();
  }
}
