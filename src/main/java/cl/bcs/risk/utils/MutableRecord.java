package cl.bcs.risk.utils;

import cl.bcs.risk.pipeline.Record;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Registro mutable que permite modificar valores y agregar columnas.
 *
 * Todos los iteradores de esta clase devuelven los valores en orden correcto.
 *
 * @author Alberto Hormazabal Cespedes
 * @author exaTech Ingenieria SpA. (info@exatech.cl)
 */
public class MutableRecord
    implements Record {

  private final List<String> keyList;

  private final Map<String, String> valuesMap;

  public MutableRecord() {
    keyList = new ArrayList<>();
    valuesMap = new HashMap<>();
  }

  public MutableRecord(Record record) {
    keyList = new ArrayList<>(record.size());
    valuesMap = new HashMap<>(record.size());
    record.entries().forEach(entry -> append(entry.getKey(), entry.getValue()));
  }


  @Override
  public int size() {
    return keyList.size();
  }

  @Override
  public String get(int index) {
    return valuesMap.get(keyList.get(index));
  }

  @Override
  public String get(String key) {
    return valuesMap.get(key);
  }

  public void set(int index, String value) {
    valuesMap.put(keyList.get(index), value);
  }

  public void set(String key, String value) {
    if (!valuesMap.containsKey(key))
      throw new NoSuchElementException(key);
    valuesMap.put(key, value);
  }

  public void append(String key, String value) {
    if (valuesMap.containsKey(key)) {
      throw new IllegalArgumentException("key already exists. use set()");
    }
    keyList.add(key);
    valuesMap.put(key, value);
  }

  public void insert(int index, String key, String value) {
    if (valuesMap.containsKey(key)) {
      throw new IllegalArgumentException("key already exists. use set()");
    }
    keyList.add(index, key);
    valuesMap.put(key, value);
  }


  @Override
  public Collection<Map.Entry<String, String>> entries() {
    return Collections.unmodifiableCollection(keyList.stream()
        .map(RecordEntry::new)
        .collect(Collectors.toList()));
  }

  @Override
  public Collection<String> keys() {
    return Collections.unmodifiableCollection(keyList);
  }

  @Override
  public Collection<String> values() {
    return Collections.unmodifiableCollection(keyList.stream()
        .map(valuesMap::get)
        .collect(Collectors.toList()));
  }

  @Override
  public Iterator<String> iterator() {
    return values().iterator();
  }


  public final class RecordEntry implements Map.Entry<String, String> {
    private String key;

    private RecordEntry(String key) {
      this.key = key;
    }

    @Override
    public String getKey() {
      return key;
    }

    @Override
    public String getValue() {
      return valuesMap.get(key);
    }

    @Override
    public String setValue(String value) {
      return valuesMap.put(key, value);
    }
  }
}
