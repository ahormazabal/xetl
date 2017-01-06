package cl.bcs.risk.steps;

import cl.bcs.risk.pipeline.*;
import cl.bcs.risk.utils.CharacterStreamReader;
import org.postgresql.copy.CopyManager;

import java.io.*;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.Map;
import java.util.stream.Stream;

/**
 * @author Alberto Hormazabal Cespedes
 * @author exaTech Ingenieria SpA. (info@exatech.cl)
 */
public class SaveDB extends AbstractStep
    implements FinalStep {

  private static String DELIM = ";";

  private CopyManager copyManager;

  private String destTable;

  @Override
  public String getType() {
    return "savedb";
  }

  @Override
  public void initialize(Pipeline pipeline, Map<String, String> properties) throws Exception {
    super.initialize(pipeline, properties);

    destTable = getRequiredProperty("dest_table");

  }

  @Override
  public void finish(Stream<? extends Record> recordStream) {
    System.out.println("FINISH HIM");

    Stream<Character> charStream = recordStream
        .map(this::recordToDbCSV)
        .flatMap(s -> s.chars().mapToObj(i -> (char) i));

    String query = String.format("COPY %s FROM STDIN WITH(DELIMITER '%s', FORMAT CSV",
        destTable, DELIM);

    try {

      BufferedReader br = new BufferedReader(new CharacterStreamReader(charStream));

      String weze;
      while ((weze = br.readLine()) != null) {
        System.out.println(weze);
      }

//      copyManager.copyIn(query, new CharacterStreamReader(charStream));

    } catch (Exception e) {
      throw new RuntimeException("Error writing data to database: " + e.getMessage(), e);
    }
  }

  private String recordToDbCSV(Record record) {
    return String.join(DELIM, record).concat("\n");
  }
}
