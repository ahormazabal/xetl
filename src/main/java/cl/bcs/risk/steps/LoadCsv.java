package cl.bcs.risk.steps;

import cl.bcs.risk.pipeline.AbstractStep;
import cl.bcs.risk.pipeline.BeginStep;
import cl.bcs.risk.pipeline.Pipeline;
import cl.bcs.risk.pipeline.Record;
import cl.bcs.risk.utils.MutableOrderedRecord;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import java.io.*;
import java.util.*;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * @author Alberto Hormazabal Cespedes
 * @author exaTech Ingenieria SpA. (info@exatech.cl)
 */
public class LoadCsv extends AbstractStep
    implements BeginStep {

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
    // Create base stream from CSV file.
    return StreamSupport.stream(csvParser.spliterator(), false)
        .map(this::toRecord);
  }

  private Record toRecord(CSVRecord record) {
    Map<String, String> fieldMap = new LinkedHashMap<>();
    csvParser.getHeaderMap().entrySet().forEach(entry -> {
      fieldMap.put(entry.getKey(), record.get(entry.getValue()));
    });
    return new MutableOrderedRecord(fieldMap);
  }

}
