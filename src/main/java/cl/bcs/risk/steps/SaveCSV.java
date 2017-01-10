package cl.bcs.risk.steps;

import cl.bcs.risk.pipeline.AbstractBaseStep;
import cl.bcs.risk.pipeline.FinalStep;
import cl.bcs.risk.pipeline.Pipeline;
import cl.bcs.risk.pipeline.Record;
import cl.bcs.risk.utils.CharacterStreamReader;
import cl.bcs.risk.utils.MutableRecord;
import org.postgresql.copy.CopyManager;
import org.postgresql.core.BaseConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;
import java.util.stream.Stream;

/**
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

  private volatile boolean headerWritten;

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

    destFile = new File(destination);
    if (!destFile.createNewFile()) {
      throw new IOException("File already exists: " + destFile);
    }
  }

  @Override
  public void finish(Stream<? extends Record> recordStream) {
    LOG.info("Writing stream to file: " + destFile);

    Stream<Character> charStream = recordStream
        .peek(this::writeHeader)
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
    return String.join(delimiter, record).concat("\n");
  }

  private void writeHeader(Record r) {
    try {
      if (!headerWritten) {
        if (dataWriter == null) {
          throw new IOException("Data Writer not opened");
        }
        // write header
        String header = String.join(delimiter, r.keys()).concat("\n");
        dataWriter.write(header);
        headerWritten = true;
      }
//      else {
//        // check valid header
//      }
    } catch (Exception e) {
      throw new RuntimeException("Error generating header", e);
    }
  }
}
