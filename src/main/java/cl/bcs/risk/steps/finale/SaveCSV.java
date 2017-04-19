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
 * Operacion terminal para grabar los registros a archivo CSV.
 *
 * @author Alberto Hormazabal Cespedes
 * @author exaTech Ingenieria SpA. (info@exatech.cl)
 */
public class SaveCSV extends AbstractBaseStep
    implements FinalStep {

  private static final Logger LOG = LoggerFactory.getLogger(SaveCSV.class);

  private static String DEFAULT_DELIMITER = ";";

  private String destination;

  private File destFile;

  private String delimiter;

  private volatile boolean headerWritten = false;

  private boolean writeHeader;

  private Writer dataWriter = null;

  @Override
  public String getType() {
    return "savecsv";
  }

  @Override
  public void initialize(Pipeline pipeline, Map<String, String> properties) throws Exception {
    super.initialize(pipeline, properties);

    destination = getRequiredProperty("destination");
    delimiter = getOptionalProperty("delimiter", DEFAULT_DELIMITER);
    writeHeader = Boolean.valueOf(getOptionalProperty("write-header", "true"));
    destFile = new File(destination);
  }

  @Override
  public void finish(Stream<? extends Record> recordStream) {
    LOG.info("Writing stream to file: " + destFile);

    try {

      if (destFile.exists()) {
        LOG.warn("Replacing existing file: " + destFile.getCanonicalPath());
        destFile.delete();
      }



      if (!destFile.createNewFile()) {
        throw new RuntimeException("File already exists: " + destFile);
      }
    } catch (Exception e) {
      throw new RuntimeException("Error creating file: " + e.getMessage(), e);
    }

    if (writeHeader) {
      recordStream = recordStream.peek(this::writeHeader);
    }

    Stream<Character> charStream = recordStream
        .map(this::recordToCSV)
        .flatMap(s -> s.chars().mapToObj(i -> (char) i));

    try (Writer writer = new OutputStreamWriter(new BufferedOutputStream(new FileOutputStream(destFile)));
         Reader dataReader = new CharacterStreamReader(charStream)) {
      dataWriter = writer;
      char[] buffer = new char[4092];
      int charRead = 0;
      while ((charRead = dataReader.read(buffer)) != -1) {
        writer.write(buffer, 0, charRead);
      }
    } catch (Exception e) {
      throw new RuntimeException(String.format("Error writing data to file %s: " + e.getMessage(), destination), e);
    }
  }

  private String recordToCSV(Record record) {
    StringJoiner j = new StringJoiner(delimiter, "", "\n");
    record.forEach(f -> j.add(f == null ? "" : f));
    return j.toString();
  }

  private void writeHeader(Record r) {
    if (!headerWritten) {
      try {
        // write header
        String header = String.join(delimiter, r.keys()).concat("\n");
        dataWriter.write(header);
        headerWritten = true;
      } catch (Exception e) {
        throw new RuntimeException("Error generating header", e);
      }
    }
  }
}
