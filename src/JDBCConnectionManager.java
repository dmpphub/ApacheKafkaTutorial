import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

/**
 * The Class JDBCConnectionManager.
 */
public final class JDBCConnectionManager {

    /** The connection. */
    private Connection connection;

    /**
     * Gets the connection.
     *
     * @return the connection
     */
    public Connection getConnection() {
        return connection;
    }

    /**
     * Sets the connection.
     *
     * @param connection the connection to set
     */
    public void setConnection(Connection connection) {
        this.connection = connection;
    }

    /**
     * Gets the jDBC connection.
     *
     * @return the jDBC connection
     */
    public boolean getJDBCConnection() {
        boolean connectionFlag = false;
        try {
            Class.forName("oracle.jdbc.driver.OracleDriver");
            connection = DriverManager.getConnection("jdbc:oracle:thin:@localhost:1521:xe", "system", "welcome");

            if (connection != null) {
                connectionFlag = true;
            }

        } catch (Exception exception) {
            exception.printStackTrace();
        }
        return connectionFlag;
    }

    /**
     * Close connection.
     *
     * @param connection the connection
     * @param statement the statement
     * @param resultSet the result set
     */
    public void closeConnection(Connection connection, Statement statement, ResultSet resultSet) {
        try {
            if (resultSet != null) {
                resultSet.close();
            }

            if (statement != null) {
                statement.close();
            }

            if ((connection != null) && (!(connection.isClosed()))) {
                connection.close();
            }
        } catch (Exception exception) {
            exception.printStackTrace();
        }

    }
}
