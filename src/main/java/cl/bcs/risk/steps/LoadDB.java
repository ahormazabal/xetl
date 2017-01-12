package cl.bcs.risk.steps;

import cl.bcs.risk.DataSourceManager;
import cl.bcs.risk.pipeline.AbstractBaseStep;
import cl.bcs.risk.pipeline.BeginStep;
import cl.bcs.risk.pipeline.Pipeline;
import cl.bcs.risk.pipeline.Record;
import cl.bcs.risk.utils.MutableRecord;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.dbutils.QueryRunner;
import org.postgresql.copy.CopyManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.transform.Result;
import java.io.*;
import java.sql.*;
import java.util.*;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * Loads a DB query into a Record Stream.
 *
 * @author Alberto Hormazabal Cespedes
 * @author exaTech Ingenieria SpA. (info@exatech.cl)
 */
public class LoadDB extends AbstractBaseStep
    implements BeginStep {

  private static final Logger LOG = LoggerFactory.getLogger(LoadDB.class);

  private String origin;
  private String datasource;


  @Override
  public String getType() {
    return "loaddb";
  }

  @Override
  public void initialize(Pipeline pipeline, Map<String, String> properties) throws Exception {
    super.initialize(pipeline, properties);

    origin = getRequiredProperty("origin");
    datasource = getOptionalProperty("datasource", DataSourceManager.DEFAULT_DATASOURCE);

  }

  @Override
  public Stream<? extends Record> begin() {
    LOG.info("Begin loading records from database origin: " + origin);

    try (Connection cnx = getPipeline().getContext().getDataSourceManager().getConnection(datasource)) {

      ResultSet rs = cnx.prepareStatement(origin).executeQuery();

      Iterator<Record> rsIterator = new Iterator<Record>() {
        @Override
        public boolean hasNext() {
          try {
            return !rs.isLast();
          } catch (SQLException e) {
            throw new RuntimeException(e);
          }
        }

        @Override
        public Record next() {
          try {
            rs.next();
            return toRecord(rs);
          } catch (SQLException e) {
            throw new RuntimeException(e);
          }
        }
      };

      return StreamSupport.stream(
          Spliterators.spliteratorUnknownSize(rsIterator, Spliterator.ORDERED),
          false);

    } catch (Exception e) {
      throw new RuntimeException("Error reading data from database: " + e.getMessage(), e);
    }
  }

  private Record toRecord(ResultSet rs) throws SQLException {
    ResultSetMetaData meta = rs.getMetaData();
    int cols = meta.getColumnCount();
    MutableRecord record = new MutableRecord();
    for (int i = 1; i <= cols; i++) {
      record.append(meta.getColumnLabel(i), rs.getString(i));
    }
    return record;
  }

}