package cl.bcs.risk.steps.finale;

import cl.bcs.risk.DataSourceManager;
import cl.bcs.risk.pipeline.AbstractBaseStep;
import cl.bcs.risk.pipeline.FinalStep;
import cl.bcs.risk.pipeline.Pipeline;
import cl.bcs.risk.pipeline.Record;
import org.apache.commons.dbutils.QueryRunner;
import org.postgresql.core.BaseConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map;
import java.util.stream.Stream;

/**
 * @author Alberto Hormazabal Cespedes
 * @author exaTech Ingenieria SpA. (info@exatech.cl)
 */
public class BatchUpsert extends AbstractBaseStep
    implements FinalStep {

  private static final Logger LOG                = LoggerFactory.getLogger(BatchUpsert.class);
  private static final String DEFAULT_BATCH_SIZE = "500";

  private String  dataSource;
  private String  destination;
  private Integer batch_size;

  private String                upsertQuery;
  private Connection            sqlConnection;
  private ArrayList<ColumnData> tableColumns;

  private QueryRunner queryRunner;

  private Object[][] paramCache;
  int paramCacheIndex;

  @Override
  public String getType() {
    return "batchupsert";
  }

  @Override
  public void initialize(Pipeline pipeline, Map<String, String> properties) throws Exception {
    super.initialize(pipeline, properties);
    LOG.info("Initializing Postgres Batch Upsert Step.");

    boolean enableUpsert = Boolean.valueOf(getOptionalProperty("enable_upsert", "true"));
    destination = getRequiredProperty("destination");
    dataSource = getOptionalProperty("datasource", DataSourceManager.DEFAULT_DATASOURCE);
    batch_size = Integer.parseInt(getOptionalProperty("batch_size", DEFAULT_BATCH_SIZE));

    sqlConnection = getPipeline().getContext().getDataSourceManager().getConnection(dataSource);
    queryRunner = new QueryRunner();

    // Analyze table schema to build query.
    String pk_id = queryRunner.query(sqlConnection,
        "SELECT constraint_name FROM information_schema.table_constraints WHERE table_name = ? AND constraint_type = 'PRIMARY KEY'",
        rs -> rs.next() ? rs.getString("constraint_name") : null,
        destination);

    if (pk_id == null || pk_id.isEmpty()) {
      throw new IllegalArgumentException("No se pudo determinar la llave primaria de la tabla: " + destination);
    }

    this.tableColumns = queryRunner.query(sqlConnection,
        "SELECT column_name, data_type FROM information_schema.columns WHERE table_name = ? ORDER BY ordinal_position",
        rs -> {
          ArrayList<ColumnData> l = new ArrayList<>();
          while (rs.next()) {
            ColumnData cd = new ColumnData();
            cd.name = rs.getString("column_name");
            cd.dataType = rs.getString("data_type");
            l.add(cd);
          }
          return l;
        }, destination);


    // Build column list
    StringBuilder columnListBuilder = new StringBuilder("(");
    for (int i = 0; i < tableColumns.size(); i++) {
      columnListBuilder.append(tableColumns.get(i).name);
      if (i < (tableColumns.size() - 1)) {
        columnListBuilder.append(",");
      }
    }
    columnListBuilder.append(")");
    String columnList = columnListBuilder.toString();

    //Build query.
    // INSERT INTO table (col1, ...) VALUES

    StringBuilder sb = new StringBuilder();
    sb.append("INSERT INTO ").append(destination).append(" ");
    sb.append(columnList).append(" VALUES ");

    // add ( ?, ?, ...)
    sb.append("(");
    for (int i = 0; i < tableColumns.size(); i++) {
      sb.append("? :: " + tableColumns.get(i).dataType);
      if (i < (tableColumns.size() - 1)) {
        sb.append(",");
      }
    }
    sb.append(")");

    if (enableUpsert) {
      // ON CONFLICT ON CONSTRAINT pk_key_constraint DO UPDATE SET (col1, ...) =
      sb.append(" ON CONFLICT ON CONSTRAINT ").append(pk_id).append(" DO UPDATE SET ");
      sb.append(columnList).append(" = ");
      // value list with and excluded

      // (excluded.col1, ...)
      sb.append("(");
      for (int i = 0; i < tableColumns.size(); i++) {
        sb.append("EXCLUDED.").append(tableColumns.get(i).name);
        if (i < (tableColumns.size() - 1)) {
          sb.append(",");
        }
      }
      sb.append(")");
    }

    upsertQuery = sb.toString();

    LOG.info("Postgres Batch Upsert Step initialized");
  }

  @Override
  public void finish(Stream<? extends Record> recordStream) {
    LOG.info("Writing stream to datasource: " + dataSource);
//    String query = String.format("COPY %s FROM STDIN WITH(DELIMITER '%s', FORMAT CSV)", destination, delimiter);

    LOG.info("Using query: " + upsertQuery);

//    Stream<Character> charStream = recordStream
//        .map(this::recordToDbCSV)
//        .flatMap(s -> s.chars().mapToObj(i -> (char) i));

    try {

      if (!(sqlConnection instanceof BaseConnection)) {
        throw new SQLException("Upserter Step only works with postgres datasources.");
      }

      // BEGIN TRANSACTION.
      sqlConnection.setAutoCommit(false);
      resetParamCache();

      recordStream.map(record -> {
        Object[] oArr = new Object[tableColumns.size()];
        for (int i = 0; i < tableColumns.size(); i++) {
          oArr[i] = record.get(i);
        }
        return oArr;
      }).forEach(params -> {

        if (paramCacheIndex == batch_size) {
          try {
            flushCache();
          } catch (SQLException e) {
            throw new RuntimeException();
          }
        }

        paramCache[paramCacheIndex] = params;
        paramCacheIndex++;
      });

      flushCache();

      // END TRANSACTION
      sqlConnection.commit();
    } catch (Exception e) {
      LOG.error("Error writing data to database: " + e.getMessage(), e);
      try {
        sqlConnection.rollback();
      } catch (SQLException e1) {
        LOG.error("Error rolling back transaction: " + e1.getMessage(), e1);
      }
      throw new RuntimeException("Error writing data to database: " + e.getMessage(), e);
    }
  }

//  private String recordToDbCSV(Record record) {
//    return String.join(delimiter, record).concat("\n");
//  }

  private void flushCache() throws SQLException {
    Object[][] batchParams = Arrays.copyOf(paramCache, paramCacheIndex);
    resetParamCache();

    if (batchParams.length > 0) {
      queryRunner.insertBatch(sqlConnection,
          upsertQuery,
          rs -> {
            while (rs.next()) {
              LOG.error("Le llea rs" + rs.getMetaData().getColumnName(1));
              LOG.error("Le llea rs" + rs.getString(1));
            }
            return null;
          }, batchParams);
    }
  }

  private void resetParamCache() {
    paramCache = new Object[batch_size][tableColumns.size()];
    paramCacheIndex = 0;
  }


  private class ColumnData {
    String name;
    String dataType;
  }

}
