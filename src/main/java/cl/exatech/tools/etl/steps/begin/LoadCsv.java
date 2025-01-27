package cl.exatech.tools.etl.steps.begin;

import cl.exatech.tools.etl.pipeline.AbstractBaseStep;
import cl.exatech.tools.etl.pipeline.BeginStep;
import cl.exatech.tools.etl.pipeline.Pipeline;
import cl.exatech.tools.etl.pipeline.Record;
import cl.exatech.tools.etl.utils.MutableRecord;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * @author Alberto Hormazabal Cespedes
 * @author exaTech Ingenieria SpA. (info@exatech.cl)
 */
public class LoadCsv extends AbstractBaseStep
    implements BeginStep {

  private static final Logger LOG = LoggerFactory.getLogger(LoadCsv.class);

  private static final int FILE_BUFFER_SIZE = 1024 * 1024 * 5; // 5MB

  private File      inputFile;
  private Character delimiter;
  private CSVParser csvParser;


  @Override
  public String getType() {
    return "loadcsv";
  }

  @Override
  public void initialize(Pipeline pipeline, Map<String, String> properties) throws Exception {
    super.initialize(pipeline, properties);

    String filePath = Objects.requireNonNull(getProperties().get("file"), "file");
    String delim = Objects.requireNonNull(getProperties().get("delimiter"), "delimiter");

    if (delim.length() != 1) {
      throw new IllegalArgumentException("delimiter must be only one character");
    }

    delimiter = delim.charAt(0);

    inputFile = new File(filePath);
    if (!inputFile.exists() || !inputFile.isFile() || !inputFile.canRead()) {
      throw new IOException("Could not open file: " + filePath);
    }

    Reader fileReader = new InputStreamReader(new BufferedInputStream(new FileInputStream(inputFile), FILE_BUFFER_SIZE));
    csvParser = new CSVParser(fileReader, CSVFormat
        .newFormat(delimiter)
        .withFirstRecordAsHeader());

  }

  @Override
  public Stream<? extends Record> begin() {
    LOG.info("Begin loading records from file: " + inputFile);

    // Create base stream from CSV file.
    return StreamSupport.stream(csvParser.spliterator(), false)
        .map(this::toRecord);
  }

  private Record toRecord(CSVRecord record) {

    MutableRecord r = new MutableRecord();
    csvParser.getHeaderMap().entrySet().forEach(entry -> {
      String key = entry.getKey();
      String val = record.get(entry.getValue());
      r.append(entry.getKey().toLowerCase().trim(), val == null ? null : val.trim());
    });
    return r;
  }

}
