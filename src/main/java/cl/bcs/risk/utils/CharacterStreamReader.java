package cl.bcs.risk.utils;

import java.io.IOException;
import java.io.Reader;
import java.util.Iterator;
import java.util.stream.Stream;

/**
 * Convierte un {@link Stream} de caracteres en un Reader.
 * <p>El uso de este reader ejecutara una operacion terminal en el stream entregado. Adicionalmente
 * si se cierra este Reader por medio de {@link #close()}, se cerrara tambien el stream.</p>
 *
 *
 * @author Alberto Hormazabal Cespedes
 * @author exaTech Ingenieria SpA. (info@exatech.cl)
 */
public class CharacterStreamReader
    extends Reader {

  private final Stream<Character> charStream;

  private final Iterator<Character> charIterator;

  /**
   * Construye un Reader desde el stream entregado.<br>
   * Esta es una operacion terminal para el stream.
   *
   * @param charStream Stream de caracteres a convertir a Reader.
   */
  public CharacterStreamReader(Stream<Character> charStream) {
    this.charStream = charStream;
    this.charIterator = charStream.iterator();
  }

  @Override
  public int read(char[] cbuf, int off, int len) throws IOException {
    if(!charIterator.hasNext()) {
      return -1;
    }
    int count = 0;
    while (count < len && charIterator.hasNext()) {
      cbuf[off + count] = charIterator.next();
      count++;
    }
    return count;
  }

  @Override
  public void close() throws IOException {
    charStream.close();
  }
}
