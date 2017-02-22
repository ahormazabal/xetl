package cl.bcs.risk.steps.finale;

import cl.bcs.risk.DataSourceManager;
import cl.bcs.risk.pipeline.AbstractBaseStep;
import cl.bcs.risk.pipeline.FinalStep;
import cl.bcs.risk.pipeline.Pipeline;
import cl.bcs.risk.pipeline.Record;
import cl.bcs.risk.utils.CharacterStreamReader;
import org.postgresql.copy.CopyManager;
import org.postgresql.core.BaseConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Reader;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;
import java.util.stream.Stream;

/**
 * @author Alberto Hormazabal Cespedes
 * @author exaTech Ingenieria SpA. (info@exatech.cl)
 */
public class SaveDB extends AbstractBaseStep
    implements FinalStep {

  private static final Logger LOG = LoggerFactory.getLogger(SaveDB.class);
  private static final String DEFAULT_RECORD_DELIMITER = ";";

  private String delimiter;
  private String dataSource;
  private String destination;

  @Override
  public String getType() {
    return "savedb";
  }

  @Override
  public void initialize(Pipeline pipeline, Map<String, String> properties) throws Exception {
    super.initialize(pipeline, properties);

    destination = getRequiredProperty("destination");
    dataSource = getOptionalProperty("datasource", DataSourceManager.DEFAULT_DATASOURCE);
    delimiter = getOptionalProperty("delimiter", DEFAULT_RECORD_DELIMITER);
  }

  @Override
  public void finish(Stream<? extends Record> recordStream) {
    LOG.info("Writing stream to datasource: " + dataSource);
    String query = String.format("COPY %s FROM STDIN WITH(DELIMITER '%s', FORMAT CSV)", destination, delimiter);
    LOG.info("Using query: " + query);

    Stream<Character> charStream = recordStream
        .map(this::recordToDbCSV)
        .flatMap(s -> s.chars().mapToObj(i -> (char) i));

    try (Connection dbConnection = getPipeline().getContext().getDataSourceManager().getConnection(dataSource);
         Reader dataReader = new CharacterStreamReader(charStream)) {

      if (!(dbConnection instanceof BaseConnection)) {
        throw new SQLException("SaveDB Step only works with postgres datasources (for now...)");
      }


      CopyManager copyManager = new CopyManager((BaseConnection) dbConnection);
      copyManager.copyIn(query, dataReader);
      dataReader.close();
      dbConnection.close();

    } catch (Exception e) {
      throw new RuntimeException("Error writing data to database: " + e.getMessage(), e);
    }
  }

  private String recordToDbCSV(Record record) {
    return String.join(delimiter, record).concat("\n");
  }
}
