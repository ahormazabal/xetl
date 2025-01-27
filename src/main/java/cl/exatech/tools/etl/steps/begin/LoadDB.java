package cl.exatech.tools.etl.steps.begin;

import cl.exatech.tools.etl.DataSourceManager;
import cl.exatech.tools.etl.pipeline.AbstractBaseStep;
import cl.exatech.tools.etl.pipeline.BeginStep;
import cl.exatech.tools.etl.pipeline.Pipeline;
import cl.exatech.tools.etl.pipeline.Record;
import cl.exatech.tools.etl.utils.MutableRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.Iterator;
import java.util.Map;
import java.util.Spliterator;
import java.util.Spliterators;
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

  private String  origin;
  private String  datasource;
  private Integer fetch_size;


  @Override
  public String getType() {
    return "loaddb";
  }

  @Override
  public void initialize(Pipeline pipeline, Map<String, String> properties) throws Exception {
    super.initialize(pipeline, properties);

    origin = getRequiredProperty("origin");
    datasource = getOptionalProperty("datasource", DataSourceManager.DEFAULT_DATASOURCE);
    fetch_size = Integer.parseInt(getOptionalProperty("fetch_size", "0"));

  }

  @Override
  public Stream<? extends Record> begin() {
    LOG.info("Begin loading records from database origin: " + origin);

    try {
      Connection cnx = getPipeline().getContext().getDataSourceManager().getConnection(datasource);

      try {
        cnx.setAutoCommit(false);

        PreparedStatement stmt = cnx.prepareStatement(origin);
        if(fetch_size > 0) {
          LOG.info("Record fetch size: " + fetch_size);
          stmt.setFetchSize(fetch_size);
        }

        LOG.info("Executing query");
        ResultSet rs = stmt.executeQuery();

        // Create resultset spliterator.
        Spliterator<Record> resultSetSpliterator;

        if (!rs.isBeforeFirst()) {
          // Empty Data
          resultSetSpliterator = Spliterators.emptySpliterator();
          LOG.warn("No data retrieved from database.");
        } else {
          resultSetSpliterator = Spliterators.spliteratorUnknownSize(new RecordResultSetIterator(rs), Spliterator.ORDERED);
        }

        LOG.info("Start streaming data.");
        return StreamSupport.stream(resultSetSpliterator, false)
            .onClose(() -> {
              try {
                LOG.info("Closing connection.");
                cnx.commit();
                cnx.close();
              } catch (SQLException e) {
                throw new RuntimeException("Error closing connection", e);
              }
            });

      } catch (Exception e) {
        cnx.close();
        throw e;
      }
    } catch (Exception e) {
      throw new RuntimeException("Error reading data from database: " + e.getMessage(), e);
    }
  }

  private Record toRecord(ResultSet rs) throws SQLException {
    ResultSetMetaData meta   = rs.getMetaData();
    int               cols   = meta.getColumnCount();
    MutableRecord     record = new MutableRecord();
    for (int i = 1; i <= cols; i++) {
      String label = meta.getColumnLabel(i);
      String val = rs.getString(i);
      record.append(label.trim(), val == null ? null : val.trim());
    }
    return record;
  }


  private class RecordResultSetIterator
      implements Iterator<Record> {

    private final ResultSet rs;

    public RecordResultSetIterator(ResultSet rs) {
      this.rs = rs;
    }

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

  }

}
