package cl.bcs.risk.jobs;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.postgresql.copy.CopyManager;
import org.postgresql.core.BaseConnection;

import java.io.*;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.StringJoiner;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * Job para cargar archivo de indices en BDD.
 *
 * @author Alberto Hormazabal Cespedes
 * @author exaTech Ingenieria SpA. (info@exatech.cl)
 */
public class LoadIndicesJob
 {

  private static final int FILE_BUFFER_SIZE = 1024 * 1024 * 5; // 5MB

  private static final char SEMICOLON = ';';

  private String configuration;

  private String filesPath = "inbox_r";

  private final Stream<CSVRecord> recordStream;

  private final Connection dbConnection;

  private String dbUrl = "jdbc:postgresql://localhost/bcsriskdb";


  //IN: FILE PATH/WILDACRD
  //IN TABLA INDICES
  // IN DATABASE URI
  // IN ARCHIVE
  public LoadIndicesJob(String config) throws Exception {
    this.configuration = config;
    //
    File filePath = new File(filesPath);

    File inFIle = new File(filePath, "Factores_20161128.csv");

    Reader fileReader = new InputStreamReader(new BufferedInputStream(new FileInputStream(inFIle), FILE_BUFFER_SIZE));
    CSVParser p = new CSVParser(fileReader, CSVFormat.newFormat(SEMICOLON));


    recordStream = StreamSupport.stream(p.spliterator(), false);

    Properties dbProps = new Properties();
    dbProps.setProperty("user", "tuto");
    dbProps.setProperty("password", "ppl123");
    dbConnection = DriverManager.getConnection(dbUrl, dbProps);

  }

  public void execute() throws Exception {

    // para cada archivo
    recordStream

        // Cambiar decimales de , a .
        .map(r -> {

          List<String> fields = new ArrayList<>(r.size());
          fields.forEach(s -> {
            fields.add(s.replaceAll("([[:digit:]]),([[:digit:]])", "\\1.\\2"));
          });

          return fields;
        })
        .map(fields -> {

          StringJoiner joiner = new StringJoiner(";");
          fields.forEach(joiner::add);
          return joiner.toString();
        })
        .collect(Collectors.toSet());



    // cargar en tabla
    // mover a archive


    CopyManager copyManager = new CopyManager((BaseConnection) dbConnection);


  }

  @SafeVarargs
  private static <T> Stream<? extends T> concat(Stream<? extends T>... streams) {
    return Stream.of(streams).reduce(Stream.empty(), Stream::concat);
  }
}
