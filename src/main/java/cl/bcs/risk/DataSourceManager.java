package cl.bcs.risk;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Objects;
import java.util.Properties;

/**
 * @author Alberto Hormazabal Cespedes
 * @author exaTech Ingenieria SpA. (info@exatech.cl)
 */
public class DataSourceManager {

  private static Logger LOG = LoggerFactory.getLogger(DataSourceManager.class);

  public static final String DEFAULT_DATASOURCE = "default";

  private final RiskEtl context;

  private final HashMap<String, Properties> dataSources;

  public DataSourceManager(RiskEtl context) {
    this.context = context;
    this.dataSources = new HashMap<>();
    loadConfig();
  }

  private void loadConfig() {

    // Check for default datasource configuration.
    String dbUrl = context.getMainProperties().getProperty("DATABASE_URL");
    if (dbUrl != null) {
      LOG.info("Configuring default datasource.");
      String dbUser = Objects.requireNonNull(context.getMainProperties().getProperty("DATABASE_USER"), "Missing DATABASE_USER");
      String dbPwd = context.getMainProperties().getProperty("DATABASE_PASS", "");

      Properties dsProps = new Properties();
      dsProps.setProperty("url", dbUrl);
      dsProps.setProperty("user", dbUser);
      dsProps.setProperty("password", dbPwd);

      dataSources.put(DEFAULT_DATASOURCE, dsProps);
    }
    else {
      throw new IllegalStateException("No default datasource defined.");
    }

    // FUTURE support for multiple datasources.
  }


  /**
   * Gets a new connection for the named datasource.
   *
   * @param dataSourceName Name of the datasource in the registry.
   * @return Database connection.
   * @throws SQLException On any error.
   */
  public Connection getConnection(String dataSourceName) throws SQLException {
    Properties p = dataSources.get(dataSourceName);
    return DriverManager.getConnection(p.getProperty("url"), p.getProperty("user"), p.getProperty("password"));
  }

}
