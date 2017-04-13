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
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * @author Alberto Hormazabal Cespedes
 * @author exaTech Ingenieria SpA. (info@exatech.cl)
 */
public class BatchInsert extends AbstractBaseStep
    implements FinalStep {

  private static final Logger LOG                = LoggerFactory.getLogger(BatchInsert.class);
  private static final String DEFAULT_BATCH_SIZE = "2000";

  private String  dataSource;
  private String  destination;
  private Integer batch_size;
  private String  delete_by_columns;

  private String                    insertQuery;
  private String                    deleteQuery;
  private Connection                sqlConnection;
  private ArrayList<ColumnData>     tableColumns;
  private ArrayList<ColumnData>     delCols;
  private Map<List<Object>, Object> deleteKeyCache;

  private QueryRunner queryRunner;

  private Object[][] paramCache;
  private Object[][] delParamCache;
  private int paramCacheIndex;
  private int delParamCacheIndex;

  @Override
  public String getType() {
    return "batchinsert";
  }

  @Override
  public void initialize(Pipeline pipeline, Map<String, String> properties) throws Exception {
    super.initialize(pipeline, properties);
    LOG.info("Initializing Batch Insert Step.");

    destination = getRequiredProperty("destination");
    delete_by_columns = getOptionalProperty("delete_by_columns", null);
    dataSource = getOptionalProperty("datasource", DataSourceManager.DEFAULT_DATASOURCE);
    batch_size = Integer.parseInt(getOptionalProperty("batch_size", DEFAULT_BATCH_SIZE));

    sqlConnection = getPipeline().getContext().getDataSourceManager().getConnection(dataSource);
    deleteKeyCache = new HashMap<>();
    queryRunner = new QueryRunner();

    // Analyze table schema to build query.
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
        columnListBuilder.append(", ");
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
      sb.append(getCastExpression(tableColumns.get(i).dataType));
      if (i < (tableColumns.size() - 1)) {
        sb.append(", ");
      }
    }
    sb.append(")");

    insertQuery = sb.toString();

    /*
    Build delete query
     */
    if (delete_by_columns != null) {
      // Validate columns
      Map<String, ColumnData> tcols = tableColumns.stream()
          .collect(Collectors.toMap(c -> c.name, c -> c, (c, c2) -> c));

      this.delCols = new ArrayList<>();
      String[] dColNames = delete_by_columns.split(",");
      for (String dc : dColNames) {
        ColumnData col = tcols.get(dc.trim());
        if (col == null) {
          throw new IllegalArgumentException("Column not found: " + dc);
        }
        delCols.add(col);
      }

      // Build query
      StringBuilder dbuilder = new StringBuilder();
      dbuilder.append("DELETE FROM ").append(destination).append(" ");

      String wastr = "WHERE ";
      for (int i = 0; i < delCols.size(); i++) {
        dbuilder.append(wastr).append(delCols.get(i).name)
            .append(" IS NOT DISTINCT FROM ").append(getCastExpression(delCols.get(i).dataType));
        if (i == 0) {
          wastr = " AND ";
        }
      }
      deleteQuery = dbuilder.toString();
    } else {
      deleteQuery = null;
    }

    LOG.info("Batch Insert Step initialized");
  }

  @Override
  public void finish(Stream<? extends Record> recordStream) {
    LOG.info("Writing stream to datasource: " + dataSource);
//    String query = String.format("COPY %s FROM STDIN WITH(DELIMITER '%s', FORMAT CSV)", destination, delimiter);

    LOG.info("Using query: " + insertQuery);
    if (deleteQuery != null) {
      LOG.info("Using delete query: " + deleteQuery);
    }

//    Stream<Character> charStream = recordStream
//        .map(this::recordToDbCSV)
//        .flatMap(s -> s.chars().mapToObj(i -> (char) i));

    try {

      // BEGIN TRANSACTION.
      sqlConnection.setAutoCommit(false);
      resetParamCache();

      recordStream
          .forEach(record -> {
            if (paramCacheIndex == batch_size) {
              try {
                flushCache();
              } catch (SQLException e) {
                throw new RuntimeException(e.getMessage(), e);
              }
            }

            checkDeleteByKey(record);

            Object[] params = new Object[tableColumns.size()];
            for (int i = 0; i < tableColumns.size(); i++) {
              params[i] = record.get(i);
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


  private void checkDeleteByKey(Record record) {
    Object[] delParams = new Object[delCols.size()];
    for (int i = 0; i < delCols.size(); i++) {
      delParams[i] = record.get(delCols.get(i).name);
    }

    List<Object> key = Arrays.asList(delParams);
    if (!deleteKeyCache.containsKey(key)) {
      deleteKeyCache.put(key, null);
      delParamCache[delParamCacheIndex] = delParams;
      delParamCacheIndex++;
    }
  }

  private void flushCache() throws SQLException {
    Object[][] batchParams = Arrays.copyOf(paramCache, paramCacheIndex);
    Object[][] delParams  = Arrays.copyOf(delParamCache, delParamCacheIndex);
    resetParamCache();

    // DO DELETE
    if(delParams.length > 0){
      LOG.info("Issuing cleaning to database on: " + delParams.length + " keys...");
      queryRunner.batch(sqlConnection, deleteQuery, delParams);
    }

    if (batchParams.length > 0) {
      LOG.info("Flushing to db: " + batchParams.length + " records...");
      queryRunner.batch(sqlConnection, insertQuery, batchParams);
    }
  }

  private void resetParamCache() {
    paramCache = new Object[batch_size][tableColumns.size()];
    delParamCache = new Object[batch_size][delCols.size()];
    paramCacheIndex = 0;
    delParamCacheIndex = 0;
  }

  private String getCastExpression(String dataType) {
    switch (dataType.toLowerCase()) {
      case "character varying":
      case "text":
      case "char":
        return "? :: " + dataType;
      default:
        return "nullif(?, '') :: " + dataType;
    }
  }


  private class ColumnData {
    public ColumnData() {
    }

    String name;
    String dataType;
  }

}
