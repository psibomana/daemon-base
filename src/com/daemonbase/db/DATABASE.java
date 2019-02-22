package com.cellulant.db;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import org.apache.commons.dbcp.ConnectionFactory;
import org.apache.commons.dbcp.DriverManagerConnectionFactory;
import org.apache.commons.dbcp.PoolableConnectionFactory;
import org.apache.commons.dbcp.PoolingDriver;
import org.apache.commons.pool.ObjectPool;
import org.apache.commons.pool.impl.GenericObjectPool;

/**
 * DATABASE handler class for database reads and writes. DATABASE database
 * connections MUST be closed by calling the closeConnection() method in this
 * class.
 *
 * @author <a href="brian.ngure@cellulant.com">Brian Ngure</a>
 * @author <a href="mailto:peter.juma@cellulant.com?Subject=Daemon">Peter
 * Bwire</a>
 */
@SuppressWarnings({
    "FinalClass",
    "CallToThreadDumpStack",
    "ClassWithoutLogger",})
public final class DATABASE {

    /**
     * The DATABASE connection pool name.
     */
    private String poolName;
    private int poolSize;

    public enum DATABASETYPE {

        MYSQL, ORACLE, POSTGRESS, DB2, SYBASE, SQLITE
    }

    /**
     * Constructor.
     *
     * @param host the DATABASE host machine
     * @param port the port to use on the DATABASE host machine
     * @param database the DATABASE database name
     * @param user the DATABASE user
     * @param password the DATABASE password
     * @param poolName the data pool name
     * @param databaseType the type of database to be used.
     *
     * @throws ClassNotFoundException if the DATABASE driver cannot be found
     * @throws InstantiationException if the DATABASE driver cannot initialised
     * @throws IllegalAccessException if there are insufficient permissions to
     * access the DATABASE driver
     * @throws SQLException if the DATABASE connection pool cannot be set up
     */
    public DATABASE(final String host, final String port, final String database,
            final String user, final String password, final String poolName, final int poolSize,
            DATABASETYPE dbType) throws ClassNotFoundException, InstantiationException,
            IllegalAccessException, SQLException {

        String databaseDriver = getDatabaseDriver(dbType);
        String databasePreconnectionURL = getConnectionURL(dbType, host, port, database, user, password);

        // Load the DATABASE driver
        Class.forName(databaseDriver).newInstance();
        this.poolName = poolName;
        this.poolSize = poolSize;

        setupDriver(databasePreconnectionURL);
    }

    /**
     * Constructor.
     *
     * @param sqliteFile the DATABASE file to be used.
     * @param database the DATABASE database name
     * @param poolName the data pool name
     *
     * @throws ClassNotFoundException if the DATABASE driver cannot be found
     * @throws InstantiationException if the DATABASE driver cannot initialised
     * @throws IllegalAccessException if there are insufficient permissions to
     * access the DATABASE driver
     * @throws SQLException if the DATABASE connection pool cannot be set up
     */
    public DATABASE(final String sqliteFile,  final String poolName, final int poolSize) throws ClassNotFoundException, InstantiationException,
            IllegalAccessException, SQLException {

        String databaseDriver = getDatabaseDriver(DATABASETYPE.SQLITE);
        String databasePreconnectionURL = getConnectionURL(DATABASETYPE.SQLITE, sqliteFile, "", "", "", "");

        // Load the DATABASE driver
        Class.forName(databaseDriver).newInstance();
        this.poolName = poolName;
        this.poolSize = poolSize;

        setupDriver(databasePreconnectionURL);
    }

    private String getConnectionURL(DATABASETYPE dbType, String host, String port, String database,
            String user, String password) {

        if (null == dbType || dbType.equals(DATABASETYPE.MYSQL)) {
            return "jdbc:mysql://" + host + ":" + port + "/" + database
                    + "?user=" + user + "&password=" + password
                    + "&autoReconnect=true&characterEncoding=UTF-8";
        } else if (dbType.equals(DATABASETYPE.ORACLE)) {
            return "jdbc:oracle:thin:" + user + "/" + password + "@" + host + ":" + port + ":" + database
                    + "&autoReconnect=true&characterEncoding=UTF-8";

        } else if (dbType.equals(DATABASETYPE.DB2)) {
            return "jdbc:db2:" + host + ":" + port + "/" + database
                    + "?user=" + user + "&password=" + password
                    + "&autoReconnect=true&characterEncoding=UTF-8";
        } else if (dbType.equals(DATABASETYPE.POSTGRESS)) {
            return "jdbc:postgresql://" + host + ":" + port + "/" + database
                    + "?user=" + user + "&password=" + password
                    + "&autoReconnect=true&characterEncoding=UTF-8";
        } else if (dbType.equals(DATABASETYPE.SYBASE)) {
            return "jdbc:sybase:Tds:" + host + ":" + port + "/" + database
                    + "?user=" + user + "&password=" + password
                    + "&autoReconnect=true&characterEncoding=UTF-8";
        } else if (dbType.equals(DATABASETYPE.SQLITE)) {
            return "jdbc:sqlite:" + host;
        } else {
            return "";
        }

    }

    public String getDatabaseDriver(DATABASETYPE dbType) {

        if (null == dbType || dbType.equals(DATABASETYPE.MYSQL)) {
            return "com.mysql.jdbc.Driver";
        } else if (dbType.equals(DATABASETYPE.ORACLE)) {
            return "oracle.jdbc.driver.OracleDriver";
        } else if (dbType.equals(DATABASETYPE.DB2)) {
            return "COM.ibm.db2.jdbc.net.DB2Driver";
        } else if (dbType.equals(DATABASETYPE.POSTGRESS)) {
            return "org.postgresql.Driver";
        } else if (dbType.equals(DATABASETYPE.SYBASE)) {
            return "com.sybase.jdbc.SybDriver";
        } else if (dbType.equals(DATABASETYPE.SQLITE)) {
            return "org.sqlite.JDBC";
        } else {
            return "";
        }
    }

    /**
     * Sets up the connection pool driver.
     *
     * @param connectURI the connection string
     *
     * @throws ClassNotFoundException on error
     * @throws SQLException on error
     */
    private void setupDriver(final String connectURI)
            throws ClassNotFoundException, SQLException {
        /*
         * First, we'll need an ObjectPool that serves as the actual pool of
         * connections.
         *
         * We'll use a GenericObjectPool instance, although any ObjectPool
         * implementation will suffice.
         *
         * Parameters here are:
         *
         * factory - the (possibly null) PoolableObjectFactory to use to create,
         *           validate and destroy objects
         * maxActive - the maximum number of objects that can be borrowed at one
         *             time (see setMaxActive(int))
         * whenExhaustedAction - the action to take when the pool is exhausted
         * maxWait - the maximum amount of time to wait (in ms) for an idle
         *           object when the pool is exhausted an and
         *           whenExhaustedAction is WHEN_EXHAUSTED_BLOCK (otherwise
         *           ignored)
         * testOnBorrow - whether or not to validate objects before they are
         *                returned by the borrowObject() method
         * testOnReturn - whether or not to validate objects after they are
         *                returned to the returnObject(java.lang.Object) method
         */
        ObjectPool<PoolableConnectionFactory> connectionPool = new GenericObjectPool<PoolableConnectionFactory>(null, poolSize,
                GenericObjectPool.WHEN_EXHAUSTED_BLOCK, 30000, true, true);

        /*
         * Next, we'll create a ConnectionFactory that the pool will use to
         * create Connections. We'll use the DriverManagerConnectionFactory,
         * using the connect string passed in.
         */
        ConnectionFactory connectionFactory = new DriverManagerConnectionFactory(connectURI, null);

        /*
         * Now we'll create the PoolableConnectionFactory, which wraps the
         * "real" Connections created by the ConnectionFactory with the classes
         * that implement the pooling functionality.
         *
         * The parameters used here are:
         *
         * connFactory - the ConnectionFactory from which to obtain base
         *               Connections
         * pool - the ObjectPool in which to pool those Connections
         * stmtPoolFactory - the KeyedObjectPoolFactory to use to create
         *                   KeyedObjectPools for pooling PreparedStatements,
         *                   or null to disable PreparedStatement pooling
         * validationQuery - a query to use to validate Connections. Should
         *                   return at least one row. Using null turns off
         *                   validation
         * validationQueryTimeout - the number of seconds that validation
         *                          queries will wait for database response
         *                          before failing. Use a value less than or
         *                          equal to 0 for no timeout
         * connectionInitSqls - a Collection of SQL statements to initialize
         *                      Connections. Using null turns off initialization
         * defaultReadOnly - the default "read only" setting for borrowed
         *                   Connections
         * defaultAutoCommit - the default "auto commit" setting for returned
         *                     Connections
         */
        PoolableConnectionFactory poolableConnectionFactory = new PoolableConnectionFactory(connectionFactory,
                connectionPool, null, "SELECT 1", 5, null, false, true);

        // Finally, we create the PoolingDriver itself...
        Class.forName("org.apache.commons.dbcp.PoolingDriver");
        PoolingDriver driver = (PoolingDriver) DriverManager
                .getDriver("jdbc:apache:commons:dbcp:");

        // ...and register our pool with it
        driver.registerPool(poolName, connectionPool);

        /*
         * Now we can just use the connect string
         *      "jdbc:apache:commons:dbcp:poolName"
         * to access our pool of Connections.
         */
    }

    /**
     * Shuts down the connection pool.
     *
     * @throws SQLException on error
     */
    public void shutdownDriver() throws SQLException {
        PoolingDriver driver = (PoolingDriver) DriverManager
                .getDriver("jdbc:apache:commons:dbcp:");
        driver.closePool(poolName);
    }

    /**
     * <p> Gets a DATABASE database connection. Will throw an SQLException if
     * there is an error. The connection uses UTF-8 character encoding. </p>
     *
     * <p> The connection is obtained using the connect string:
     * jdbc:apache:commons:dbcp:poolName </p>
     *
     * @return a DATABASE connection object, null on error
     *
     * @throws SQLException if unable to get a connection from the connection
     * pool
     */
    public Connection getConnection() throws SQLException {
        Connection conn = DriverManager
                .getConnection("jdbc:apache:commons:dbcp:" + poolName);

        return conn;
    }
}
