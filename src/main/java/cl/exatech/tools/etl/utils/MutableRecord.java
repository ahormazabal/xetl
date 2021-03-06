package cl.exatech.tools.etl.utils;

import cl.exatech.tools.etl.pipeline.Record;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Registro (tupla) mutable que permite modificar valores y agregar columnas.
 *
 * Todos los iteradores de esta clase devuelven los valores en el mismo orden en que
 * las columnas estan indexadas.
 *
 * <b>Esta clase NO ES thread-safe.</b>
 *
 * @author Alberto Hormazabal Cespedes
 * @author exaTech Ingenieria SpA. (info@exatech.cl)
 */
public class MutableRecord
    implements Record {

  private final List<String> keyList;

  private final Map<String, String> valuesMap;

  /**
   * Crea un nuevo registro vacio.
   */
  public MutableRecord() {
    keyList = new ArrayList<>();
    valuesMap = new HashMap<>();
  }

  /**
   * Crea una copia mutable del registro entregado.
   *
   * @param record Registro a copiar.
   */
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

  /**
   * Sets the value at the specified index.
   *
   * @param index The index to replace
   * @param value The new value
   * @return this same record.
   * @throws IndexOutOfBoundsException if index >= size()
   */
  public MutableRecord set(int index, String value) {
    valuesMap.put(keyList.get(index), value);
    return this;
  }

  /**
   * Sets the value mapped at key.
   *
   * @param key Key to which replace its value.
   * @param value The new value to replace.
   * @return this same record.
   * @throws NoSuchElementException if 'key' is not mapped in this record.
   */
  public MutableRecord set(String key, String value) {
    if (!valuesMap.containsKey(key))
      throw new NoSuchElementException(key);
    valuesMap.put(key, value);
    return this;
  }

  /**
   * Appends a new field to the end of this record.
   *
   * @param key The new key to map.
   * @param value The new value
   * @return this same record.
   * @throws IllegalArgumentException if key is already mapped.
   */
  public MutableRecord append(String key, String value) {
    if (valuesMap.containsKey(key)) {
      throw new IllegalArgumentException("key already exists. use set()");
    }
    keyList.add(key);
    valuesMap.put(key, value);
    return this;
  }

  /**
   * Inserts a new field into this record at position 'index'
   *
   * @param index The index at which to insert the value.
   * @param key The new key to map
   * @param value The initial value.
   * @return this same record.
   * @throws IllegalArgumentException If key is already mapped in this record.
   */
  public MutableRecord insert(int index, String key, String value) {
    if (valuesMap.containsKey(key)) {
      throw new IllegalArgumentException("key already exists. use set()");
    }
    keyList.add(index, key);
    valuesMap.put(key, value);
    return this;
  }

  /**
   * Removes element at position <i>index</i>
   *
   * @param index
   * @return Removed value;
   */
  public String remove(int index) {
    String val = valuesMap.remove(keyList.get(index));
    keyList.remove(index);
    return val;
  }

  /**
   * Removes element represented by <i>key</i>.
   *
   * @param key
   * @return Removed value;
   */
  public String remove(String key) {
    if (!valuesMap.containsKey(key))
      throw new NoSuchElementException(key);

    keyList.remove(key);
    return valuesMap.remove(key);
  }


  @Override
  public boolean containsKey(String key) {
    return valuesMap.containsKey(key);
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

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder("MutableRecord{");
    entries().forEach(entry -> {
      sb.append(entry.getKey()).append("=").append(entry.getValue()).append(", ");
    });
    sb.append('}');
    return sb.toString();
  }

  /**
   * Our implementation of Map.Entry so we can return entries with the same fashion
   * as {@link Map}.
   */
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
