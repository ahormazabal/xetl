package cl.bcs.risk.steps.finale;

import cl.bcs.risk.pipeline.AbstractBaseStep;
import cl.bcs.risk.pipeline.FinalStep;
import cl.bcs.risk.pipeline.Pipeline;
import cl.bcs.risk.pipeline.Record;
import cl.bcs.risk.utils.CharacterStreamReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.Map;
import java.util.StringJoiner;
import java.util.stream.Stream;

/**
 * @author Alberto Hormazabal Cespedes
 * @author exaTech Ingenieria SpA. (info@exatech.cl)
 */
public class StdoutFinalStep extends AbstractBaseStep
    implements FinalStep {

  private static final Logger LOG = LoggerFactory.getLogger(StdoutFinalStep.class);

  private static String DEFAULT_DELIMITER = ";";

  private String delimiter;

  private volatile boolean header;

  private Writer dataWriter = null;

  @Override
  public String getType() {
    return "stdout";
  }

  @Override
  public void initialize(Pipeline pipeline, Map<String, String> properties) throws Exception {
    super.initialize(pipeline, properties);

    delimiter = getOptionalProperty("delimiter", DEFAULT_DELIMITER);
  }

  @Override
  public void finish(Stream<? extends Record> recordStream) {
    LOG.info("Writing stream to stdout");

    Stream<Character> charStream = recordStream
        .peek(this::writeHeader)
        .map(this::recordToCSV)
        .flatMap(s -> s.chars().mapToObj(i -> (char) i));


    try (Writer writer = new OutputStreamWriter(new BufferedOutputStream(System.out));
         Reader dataReader = new CharacterStreamReader(charStream)) {
      dataWriter = writer;
      char[] buffer = new char[4092];
      int charRead = 0;
      while ((charRead = dataReader.read(buffer)) != -1) {
        writer.write(buffer, 0, charRead);
      }
    } catch (Exception e) {
      throw new RuntimeException("Error writing data to stdout: " + e.getMessage(), e);
    }
  }

  private String recordToCSV(Record record) {
    StringJoiner j = new StringJoiner(delimiter, "", "\n");
    record.forEach(f -> j.add(f == null ? "" : f));
    return j.toString();
  }

  private void writeHeader(Record r) {
    if (!header) {
      try {
        String header = String.join(delimiter, r.keys()).concat("\n");
        dataWriter.write(header);
        this.header = true;
      } catch (Exception e) {
        throw new RuntimeException("Error generating header", e);
      }
    }
  }
}
