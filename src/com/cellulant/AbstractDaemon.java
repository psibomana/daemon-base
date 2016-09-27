package com.cellulant;

import com.cellulant.db.DATABASE;
import com.cellulant.utils.AbstractProps;
import com.cellulant.utils.DaemonConstants;
import com.cellulant.utils.Logging;
import com.cellulant.utils.ResultApiFailureHandler;
import java.io.*;
import java.net.Socket;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

/**
 * <p>Stabilized parent class 26/03/12.</p> <p>This design is based on the
 * framework of the ChannelRequestPusher daemon and the original version of the
 * ExternalTransactionsProcessor(AbstractDaemon daemon).</p> <p>This is the
 * Daemon's main class. When loaded, it runs continously checking the database
 * for any requests. When a request is found, it queues the request to the job
 * class which formulates the RPC payload and finally sends it to the collectors
 * API.</p>
 *
 * <p>< FUTURE AMMENDS > Fork-Join principle(JAVA SE 7) ---> Split multiple
 * tasks across multiple processors so that we can execute tasks in
 * parallel.</p>
 *
 * Cellulant Ltd
 *
 * @author <a href="kim.kiogora@cellulant.com">Kim Kiogora</a>
 * @author <a href="brian.ngure@cellulant.com">Brian Ngure</a>
 * @author <a href="mailto:peter.juma@cellulant.com?Subject=Daemon">Peter
 * Bwire</a>
 *
 * @version Version 3.0
 */
public abstract class AbstractDaemon {

    private final String MAIN_DAEMON_TABLE;
    /**
     * The MySQL data source.
     */
    public DATABASE database;
    /**
     * File input stream to check for failed queries.
     */
    private FileInputStream fin;
    /**
     * Data input stream to check for failed queries.
     */
    private DataInputStream in;
    /**
     * Buffered reader to check for failed queries.
     */
    private BufferedReader br;
    /**
     * The job thread pool.
     */
    protected ThreadPool threadPool;
    /**
     * The daemons current state.
     */
    private int daemonState;
    /**
     * System properties class instance.
     */
    public AbstractProps props;
    /**
     * Log class instance.
     */
    public Logging log;
    /**
     * The current run ID.
     */
    public int runID;
    private ResultApiFailureHandler resultApiFailureHandler;

    /**
     * Constructor. Checks for any errors while loading system properties,
     * creates the thread pool and resets partially processed records.
     *
     * @param props the loaded system properties
     * @param logging
     * @param database the Database connection pool resource
     * @param maintable the main table from which daemon picks requests.
     */
    public AbstractDaemon(final AbstractProps props, final Logging logging,
            final DATABASE database, final String maintable) {
        this.props = props;
        this.log = logging;
        this.database = database;
        this.MAIN_DAEMON_TABLE = maintable;
        this.resultApiFailureHandler = new ResultApiFailureHandler(props, logging, maintable);

        // Set the initial run id
        runID = props.getStartupRunID();

        // Get the list of errors found when loading system properties
        List<String> loadErrors = props.getLoadErrors();
        int sz = loadErrors.size();
        if (sz > 0) {
            logging.info(getLogPreString() + "There were exactly " + sz
                    + " error(s) during the load operation...");

            System.out.println(getLogPreString() + "There were exactly " + sz
                    + " error(s) during the load operation...");

            for (String err : loadErrors) {
                System.out.println(err);
                logging.error(getLogPreString() + err);
            }

            logging.info(getLogPreString() + "Unable to start daemon because "
                    + sz + " error(s) occured during load. See log files...");
            System.exit(1);
        } else {
            logging.info(getLogPreString()
                    + "All required properties were loaded successfully");

            // Create the executor service with a fixed thread pool size
            threadPool = new ThreadPool(props.getNumOfChildren(), logging);

            logging.info(getLogPreString()
                    + "Checking whether the database is up and running");

            while (true) {
                int pingState = pingDatabaseServer();

                if (pingState == DaemonConstants.PING_SUCCESS) {
                    resetPartiallyProcessedRecords();
                    daemonState = DaemonConstants.DAEMON_RUNNING;
                    return;
                } else {
                    logging.fatal(getLogPreString() + "Unable to start daemon "
                            + "because the database is not responding...");
                }

                doWait(props.getSleepTime());
            }
        }
    }

    /**
     * Method <i>resetPartiallyProcessedRecords</i> is called by Father when he
     * wakes up to reset all records that were partially processed. Maybe
     * because the Application was killed or died prematurely(NOT GOOD).
     */
    public void resetPartiallyProcessedRecords() {
        log.info(getLogPreString() + "Checking for partially processed records...");

        
        String updateQuery = "UPDATE " + MAIN_DAEMON_TABLE + " SET bucketID = 0 WHERE "
                + "bucketID >= ? AND (processed = ? OR processed is NULL)";

        List<Object> params = new ArrayList<Object>();
        params.add(props.getMinRunID());
        params.add(props.getUnprocessedStatus());
        
        int result = updateRecord(updateQuery, params, false);
        
         if (result > 0) {
                log.info(getLogPreString()
                        + "resetPartiallyProcessedRecords --- I have reset all "
                        + "partially processed records");
            } else {
                log.info(getLogPreString()
                        + "No PartiallyProcessedRecords found...");
            }

        
    }

    /**
     * Check if the database server is up. Added 03/07/11.
     */
    private int pingDatabaseServer() {
        int state;

        try {
            String host = props.getDbHost();
            int port = Integer.parseInt(props.getDbPort());

            Socket ps = new Socket(host, port);

            /*
             * Same time we use to sleep is the same time we use for ping wait
             * period.
             */
            ps.setSoTimeout(props.getSleepTime());

            if (ps.isConnected()) {
                state = DaemonConstants.PING_SUCCESS;
                ps.close();
            } else {
                state = DaemonConstants.PING_FAILED;
            }
        } catch (UnknownHostException se) {
            log.error(getLogPreString() + " problems ", se);
            state = DaemonConstants.PING_FAILED;
        } catch (SocketException se) {
            log.error(getLogPreString() + " problems ", se);
            state = DaemonConstants.PING_FAILED;
        } catch (IOException se) {
            log.error(getLogPreString() + " problems ", se);
            state = DaemonConstants.PING_FAILED;
        }

        return state;
    }

    /**
     * Sleeps for the specified amount of time.
     *
     * @param time the time to sleep
     */
    private void doWait(final long time) {
        try {
            Thread.sleep(time);
        } catch (InterruptedException ex) {
            log.error(getLogPreString() + " problems :  ", ex);
        }
    }

    public int allocateBucket(final int currentRunID) {
        return allocateBucket(currentRunID, false);
    }

    /**
     * Method <i>allocateBucket</i> allocates a bucket for father to pick and
     * put in the processing queue.
     *
     * @param currentRunID the Run ID to be used in the allocation
     * @return a status indicating if there were records allocated or not
     */
    public int allocateBucket(final int currentRunID, boolean checkProcessedNull) {
        PreparedStatement stmt = null;
        Connection conn = null;

        String updateQuery = "UPDATE " + MAIN_DAEMON_TABLE + " SET bucketID = ? WHERE "
                + "( nextSend < now() OR nextSend IS NULL ) AND "
                + "bucketID = 0 AND (processed = ?";
        updateQuery += checkProcessedNull ? " OR processed is NULL" : "";
        updateQuery += ") AND CASE ? "
                + "WHEN 'MINUTE' THEN TIMESTAMPDIFF(MINUTE, dateCreated, NOW()) < ? "
                + "WHEN 'HOUR' THEN TIMESTAMPDIFF(HOUR, dateCreated, NOW()) < ? "
                + "WHEN 'DAY' THEN TIMESTAMPDIFF(DAY, dateCreated, NOW()) < ? "
                + "ELSE TIMESTAMPDIFF(HOUR, dateCreated, NOW()) < ? "
                + "END AND numberOfSends < ? LIMIT ?";
        int result = 0;

        try {
            conn = database.getConnection();
            stmt = conn.prepareStatement(updateQuery);
            stmt.setInt(1, currentRunID);
            stmt.setInt(2, props.getUnprocessedStatus());
            stmt.setString(3, props.getExpiryTimeUnit());
            stmt.setInt(4, props.getExpiryTimeValue());
            stmt.setInt(5, props.getExpiryTimeValue());
            stmt.setInt(6, props.getExpiryTimeValue());
            stmt.setInt(7, props.getExpiryTimeValue());
            stmt.setInt(8, props.getMaxSendRetries());
            stmt.setInt(9, props.getBucketSize());

            result = stmt.executeUpdate();

            if (result > 0) {
                log.info(getLogPreString()
                        + "allocateBucket ==> Just allocated a bucket using: \n"
                        + stmt.toString() + "  \n of size : " + result);
            }
        } catch (SQLException e) {
            log.error(getLogPreString() + "allocateBucket ==> Failed to "
                    + "allocate Bucket, reason: " + e.getMessage(), e);
        } finally {
            if (stmt != null) {
                try {
                    stmt.close();
                } catch (SQLException sqlex) {
                    log.error(getLogPreString()
                            + "allocateBucket ==> Failed to close statement: "
                            + sqlex.getMessage(), sqlex);
                }
            }

            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException sqle) {
                    log.error(getLogPreString()
                            + "allocateBucket ==> Failed to close connection: "
                            + sqle.getMessage(), sqle);
                }
            }
        }

        return result;
    }

    /**
     * Method <i>allocateBucket</i> allocates a bucket for father to pick and
     * put in the processing queue.
     *
     * @param currentRunID the Run ID to be used in the allocation
     * @return a status indicating if there were records allocated or not
     */
    public int allocateBucket(final String updateQuery, List<Object> params) {
        return updateRecord(updateQuery, params, false);
    }

    /**
     * Method <i>resetBucket</i> resets a previously allocated bucket back to
     * unprocessed.
     *
     * @param runID the Run ID to be reset
     *
     * @return a status to indicate if a bucket was successfully allocated or
     * not
     */
    public int resetBucket(final int runID) {
        PreparedStatement stmt = null;
        Connection conn = null;

        String updateQuery = "UPDATE " + MAIN_DAEMON_TABLE + " SET bucketID = 0 WHERE "
                + "bucketID = ? AND processed = ?";

        int result = 0;
        try {
            conn = database.getConnection();
            stmt = conn.prepareStatement(updateQuery);
            stmt.setInt(1, runID);
            stmt.setInt(2, props.getUnprocessedStatus());

            result = stmt.executeUpdate();
            if (result > 0) {
                log.info(getLogPreString() + "resetBucket --- Just did "
                        + "reset to records with runID " + runID + " to status "
                        + props.getUnprocessedStatus());
            }
        } catch (SQLException e) {
            log.error(getLogPreString() + "resetBucket --- Failed to "
                    + "reset bucket for runID - " + runID + ", to status - "
                    + props.getUnprocessedStatus() + ". Reason: "
                    + e.getMessage());
        } finally {
            if (stmt != null) {
                try {
                    stmt.close();
                } catch (SQLException sqlex) {
                    log.error(getLogPreString() + "resetBucket ==> Failed "
                            + "to close statement: " + sqlex.getMessage());
                }
            }

            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException sqle) {
                    log.error(getLogPreString() + "resetBucket ==> Failed "
                            + "to close connection: " + sqle.getMessage());
                }
            }
        }

        return result;
    }

    /**
     * Method <i>fetchBucket</i> gets a bucket of unprocessed tasks and
     * processes them.
     */
    public abstract void fetchBucket();

    /**
     * This function determines how the queries will be re-executed i.e. whether
     * SELECT or UPDATE.
     *
     * @param query the query to re-execute
     * @param tries the number of times to retry
     */
    private void doRecon(final String query, final int tries) {
        int maxRetry = props.getMaxSendRetries();

        if (query.toLowerCase().startsWith(DaemonConstants.UPDATE_ID)) {

            int qstate = updateRecord(query);

            if (qstate == DaemonConstants.UPDATE_RECON_FAILED) {

                log.info(getLogPreString() + "Failed to re-execute failed query: " + query + "[ Try " + tries + " out of  " + maxRetry);


                if (tries < maxRetry) {

                    log.info(getLogPreString() + "Retrying in " + (props.getSleepTime() / 1000) + " sec(s) ");
                    doWait(props.getSleepTime());
                    doRecon(query, tries + 1);
                }
            }
        }
    }

    /**
     * Loads a file with selected queries and re-runs them internally.
     *
     * @param file the file to check for failed queries
     */
    @SuppressWarnings("NestedAssignment")
    private List<String> checkForFailedQueries(final String filePath) {
        List<String> queries = new ArrayList<String>(0);


        try {
            /*
             * If we fail to open the file, then the file has not been created
             * yet. This is good because it means that there is no error.
             */
            File file = new File(filePath);
            if (file.exists()) {

                if (file.length() > 0) {
                    fin = new FileInputStream(file);
                    in = new DataInputStream(fin);
                    br = new BufferedReader(new InputStreamReader(in));

                    String data;
                    while ((data = br.readLine()) != null) {
                        if (!queries.contains(data) && !data.isEmpty()) {
                            queries.add(data);
                        }
                    }

                    if (queries.size() > 0) {
                        //modification to delete contents after reading.
                        PrintWriter w = new PrintWriter(new FileWriter(file));
                        w.close();
                    }
                }
            }
        } catch (IOException e) {
            log.error(getLogPreString() + e.getMessage());

            try {
                fin.close();
            } catch (IOException ex) {
                log.error(getLogPreString() + ex.getMessage());
            }

            try {
                br.close();
            } catch (IOException ex) {
                log.error(getLogPreString() + ex.getMessage());
            }
        }

        return queries;
    }

    /**
     * Update successful transactions that were not updated.
     */
    private void rollbackSystem() {


        List<String> failedQueries = checkForFailedQueries(DaemonConstants.FAILED_QUERIES_FILE);

        if (failedQueries.size() > 0) {

            log.info(getLogPreString() + "I found " + failedQueries.size()
                    + " failed update queries in file: "
                    + DaemonConstants.FAILED_QUERIES_FILE
                    + ", rolling back transactions...");

            for (String recon_query : failedQueries) {
                doRecon(recon_query, DaemonConstants.RETRY_COUNT);
                doWait(props.getSleepTime());

            }

            log.info(getLogPreString() + "I have finished performing rollback...");
        }

    }

    /**
     * A better functional logic that ensures secure execution of fetch bucket
     * as well as detailed management of interrupted queries. This will work
     * only when we have a db connection.
     */
    private synchronized void doWork() {

        rollbackSystem();
        fetchBucket();
    }

    /**
     * Process payments.
     */
    public void runDaemon() {
        int pingState = pingDatabaseServer();
        if (pingState == DaemonConstants.PING_SUCCESS) {
            // The database is available, allocate, fetch and reset the bucket
            if (daemonState == DaemonConstants.DAEMON_RUNNING) {
                doWork();
            } else if (daemonState == DaemonConstants.DAEMON_RESUMING) {
                log.info(getLogPreString() + "Connection to the database "
                        + "was re-established, restoring service...");

                doWait(props.getSleepTime());

                int bucket = threadPool.getListSize();
                if (bucket > 0) {
                    log.info(getLogPreString() + "Performing system "
                            + "restore => clearing the work queue ...");
                    threadPool.clearQueue();
                }

                // Update successfull transactions, that were not updated
                rollbackSystem();

                /*
                 * Now, reset partially processed records so that they are
                 * picked up again.
                 */
                resetPartiallyProcessedRecords();

                log.info(getLogPreString() + "Performing system restore "
                        + "=> refreshing the work pool...");

                threadPool = new ThreadPool(props.getNumOfChildren(), log);


                if (runID > props.getMinRunID()) {
                    log.info(getLogPreString() + "Performing system restore => resetting the Current_Run_ID to "
                            + " initial run_id [ " + runID + " ]");
                    runID = props.getMinRunID();
                }

                log.info(getLogPreString() + "Resuming daemon service...");
                daemonState = DaemonConstants.DAEMON_RUNNING;
                log.info(getLogPreString()
                        + "Daemon resumed successfully, working...");
            }
        } else {
            log.error(getLogPreString() + "The database server: "
                    + props.getDbHost() + " servicing on port: "
                    + props.getDbPort() + " appears to be down. Reason: "
                    + "internal function for pingDatabaseServer() returned a "
                    + "PING_FAILED status.");
            daemonState = DaemonConstants.DAEMON_INTERRUPTED;

            log.info(getLogPreString() + "Connection to the database was "
                    + "interrupted, suspending from service...");
            log.info(getLogPreString() + "Cleaning up service...");

            int bucket = threadPool.getListSize();
            if (bucket > 0) {
                log.info(getLogPreString() + "Performing cleanup => "
                        + "clearing the threadpool queue...");
                threadPool.clearQueue();
            }

            log.info(getLogPreString() + "Performing cleanup => closed the "
                    + "threadpool...");


            log.info(getLogPreString()
                    + "Cleanup finished. Service suspended...");

            // Enter a Suspended state
            while (true) {
                if (daemonState == DaemonConstants.DAEMON_INTERRUPTED) {
                    int istate = pingDatabaseServer();
                    if (istate == DaemonConstants.PING_SUCCESS) {
                        daemonState = DaemonConstants.DAEMON_RESUMING;
                        break;
                    }
                }

                doWait(props.getSleepTime());
            }
        }
    }

    /**
     * Method <i>getCurrentRun</i> determines and returns the Current Run ID to
     * be used when allocating a bucket for the respective iteration.
     *
     * @return the Current run ID
     */
    @SuppressWarnings("ValueOfIncrementOrDecrementUsed")
    public int getCurrentRun() {
        // Check if the CURRENT_RUN_AT_STARTUP has exceeded the MAXMIMUM values
        if (runID > props.getMaxRunID()) {
            log.info(getLogPreString() + "getCurrentRun --- CRITICAL: "
                    + "Resetting RunID to " + props.getMinRunID());
            runID = props.getMinRunID();
            return runID;
        }

        return ++runID;
    }

    /**
     * Prepended text added to each log message.
     *
     * @return "AbstractDaemon | "
     */
    public final String getLogPreString() {
        return this.getClass().getSimpleName() + " | ";
    }

    /**
     * Terminate the Daemon gracefully.
     */
    public void stopDaemon() {
        log.info(getLogPreString()
                + " [ Safe Mode ] Shutdown has been requested...");
        freeResources();
    }

    /**
     * Free Resources for system shutdown.
     */
    private void freeResources() {
        log.info(getLogPreString() + "freeResources --- Checking "
                + "whether there are any tasks(Children) in the queue .... ");
        log.info(getLogPreString() + "freeResources --- Waiting for "
                + "queued jobs to complete....");
        threadPool.join();
        threadPool.close();
        log.info(getLogPreString() + "freeResources --- ThreadPool "
                + "was shutdown successfully...");
    }

    /**
     * Updates a record.
     *
     * @param updateQuery the update query
     * @return results of an update.
     */
    public int updateRecord(final String updateQuery) {
        Statement stmt = null;
        Connection conn = null;
        int result = 0;

        log.debug(getLogPreString() + " | UpdateRecord -- QUERY: " + updateQuery);

        try {
            conn = database.getConnection();
            stmt = conn.createStatement();

            result = stmt.executeUpdate(updateQuery);

            log.debug(getLogPreString() + " | UpdateRecord -- QUERY Result is : " + result);

            if (result > 0) {
                // Update  was successfull
                log.info(getLogPreString()
                        + "update Record was successfull, updateQuery => "
                        + updateQuery);
            }
        } catch (SQLException ex) {
            log.error(getLogPreString() + "Update record error: ", ex);
            log.info(getLogPreString() + "Invoking failsafe => updateFile()");
            log.error(getLogPreString() + "Update Record --- FAILED UPDATE. updateQuery was => " + updateQuery);
            updateFailedQueriesFile(DaemonConstants.FAILED_QUERIES_FILE, updateQuery);
        } finally {
            if (stmt != null) {
                try {
                    stmt.close();
                } catch (SQLException e) {
                    log.error(getLogPreString() + e.getMessage());
                }
            }

            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException e) {
                    log.error(getLogPreString() + e.getMessage());
                }
            }
        }

        return result;
    }

    /**
     * Updates a record via the use of a prepared statement.
     *
     * @param updateQuery the update query
     * @param params the parameter array
     *
     * @return results of an update.
     */
    public int updateRecord(final String updateQuery, List<Object> params) {
        return updateRecord(updateQuery, params, true);
    }

    /**
     * Updates a record via the use of a prepared statement.
     *
     * @param updateQuery the update query
     * @param params the parameter array
     * @param enableFailSafeLogging reduce verbose ness.
     * @return results of an update.
     */
    public int updateRecord(final String updateQuery, List<Object> params, boolean enableFailSafeLogging) {


        log.debug(getLogPreString() + " updateRecord |--     "
                + " Query: " + updateQuery + "    "
                + " paramerters: " + params.toString());

        PreparedStatement stmt = null;
        Connection conn = null;
        int result = 0;

        try {
            conn = database.getConnection();
            stmt = conn.prepareStatement(updateQuery);

            // ******* Loop through objects by getting their object type and
            // ******* populate the prepared statement params
            // The counter keeps a reference to index. using indexOf brings problems when u have 
            // several similar parameters in params.
            int counter = 0;
            for (Object param : params) {


                counter += 1;

                if (param instanceof Integer) {
                    stmt.setInt(counter, (Integer) param);
                } else if (param instanceof String) {
                	String cleanedStringOne = cleanString((String) param);
                    stmt.setString(counter, cleanedStringOne);
                } else if (param instanceof Float) {
                    stmt.setFloat(counter, (Float) param);
                } else if (param instanceof Date) {
                    stmt.setDate(counter, (java.sql.Date) param);
                } else if (param instanceof Boolean) {
                    stmt.setBoolean(counter, (Boolean) param);
                } else if (param instanceof Long) {
                    stmt.setLong(counter, (Long) param);
                } else {
                	String cleanedStringTwo = cleanString((String) param);
                    stmt.setString(counter, cleanedStringTwo);
                }

                if (enableFailSafeLogging) {
                    log.info(getLogPreString() + " updateRecord |--     "
                            + " Setting: index =>>" + counter + "    "
                            + " paramerter =>> " + param.toString());
                }
            }

            result = stmt.executeUpdate();

            if (result > 0) {
                // Update  was successfull

                log.debug(getLogPreString() + "Update Record was successfull, updateQuery => " + updateQuery);

            }

        } catch (SQLException ex) {

            log.error(getLogPreString() + "Update record error: ", ex);
            log.info(getLogPreString() + "Invoking failsafe => updateFile()");
            if (enableFailSafeLogging) {
                String query = prepareRowQueryFromPreparedPayload(updateQuery, params);
                updateFailedQueriesFile(DaemonConstants.FAILED_QUERIES_FILE, query);
            }

        } catch (Exception ex) {

            log.error(getLogPreString() + "Update record error: ", ex);
            log.info(getLogPreString() + "Invoking failsafe => updateFile()");
            //log.error(getLogPreString() + "updateRecord --- FAILED UPDATE. " + "updateQuery was => " +  stmt.toString());
            if (enableFailSafeLogging) {
                String query = prepareRowQueryFromPreparedPayload(updateQuery, params);
                updateFailedQueriesFile(DaemonConstants.FAILED_QUERIES_FILE, query);
            }

        } finally {
            if (stmt != null) {
                try {
                    stmt.close();
                } catch (SQLException e) {
                    log.error(getLogPreString() + e.getMessage());
                }
            }

            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException e) {
                    log.error(getLogPreString() + e.getMessage());
                }
            }
        }

        return result;
    }

    private String prepareRowQueryFromPreparedPayload(final String updateQuery, List<Object> params) {

        log.debug(getLogPreString() + " prepareRowQueryFromPreparedPayload  : initial query --  " + updateQuery);
        log.debug(getLogPreString() + " prepareRowQueryFromPreparedPayload  : parameters for query --  " + params);


        String query = updateQuery.replaceAll("[?]", "'%s'");
        query = String.format(query.replace("\\n", ""), params.toArray());

        log.debug(getLogPreString() + " prepareRowQueryFromPreparedPayload  : final query --  " + query);
        return query;
    }

    public String cleanString(String stringData) {

        stringData = stringData.replaceAll("\\\\", "");
        stringData = stringData.replaceAll("\\n", "");
        stringData = stringData.replaceAll("\\r", "");
        stringData = stringData.replaceAll("\\t", "");
        stringData = stringData.replaceAll("\\00", "");
        stringData = stringData.replaceAll("'", "\\\\'");
        stringData = stringData.replaceAll("\\\"", "\\\\\"");
        return stringData;
    }

    // FILE MANIPULATION FUNCTIONS
    /**
     * Appends the query string to the specified file.
     *
     * @param filepath the file name
     * @param data the query string
     */
    private void writeToFile(final String filepath, final String data) {
        PrintWriter pout = null;
        try {
            pout = new PrintWriter(new FileOutputStream(filepath, true));
            pout.println(data);
            pout.close();
            log.info(getLogPreString() + "Appended query: " + data
                    + " to file: " + filepath);
        } catch (FileNotFoundException e) {
            log.error(getLogPreString() + "Failed to append query: " + data
                    + " to file: " + filepath + e.getMessage());
            if (pout != null) {
                pout.close();
            }
        }
    }

    /**
     * <p>Store queries in a file. Checks whether the queries file exists and
     * writes to it the queries that need to be performed.</p> <p>NOTE: Must be
     * used where we have read, write, update and delete access.</p>
     *
     * @param file the file name
     * @param data the query to write to the file
     */
    public void updateFailedQueriesFile(final String file, final String data) {
        log.info(getLogPreString() + "FailSafe procedure invoked..., time: "
                + getDateTime());

        File queryFile = new File(file);
        log.info(getLogPreString() + "Query file access creation and appending to file..., time " + getDateTime());
        try {
            if (!queryFile.exists()) {
                queryFile.createNewFile();
            }

            writeToFile(file, data); // And write to it
            log.debug(file + "__2      :    --->  " + data); // And write to it

        } catch (IOException ex) {
            // I/O error
            log.error(getLogPreString()
                    + "The function createNewFile() generated an error: "
                    + ex.getMessage());
        } catch (SecurityException ex) {
            // Permissions error
            log.error(getLogPreString()
                    + "The function createNewFile() generated an error: "
                    + ex.getMessage());

        }
    }

    /**
     * Return the time.
     *
     * @return the time
     */
    private String getCurrentTime() {
        SimpleDateFormat sdf = new SimpleDateFormat(DaemonConstants.TIME_FORMAT);
        return sdf.format(new Date());
    }

    /**
     * Return the date plus time.
     *
     * @return the date plus time
     */
    private String getDateTime() {
        SimpleDateFormat sdf = new SimpleDateFormat(DaemonConstants.DATE_FORMAT);
        return sdf.format(new Date());
    }

    /**
     * This method checks if the maximum memory capacity has been hit and
     * returns True if the daemon should accept more jobs else it returns false.
     *
     * @return
     */
    public boolean isRecordsInStackMaximum() {

        int recordsInStack = threadPool.getListSize();
        if (recordsInStack < props.getMaxMemCapacity()) {
            log.info(getLogPreString() + " | isRecordsInStackMaximum --- We have "
                    + recordsInStack + " records instack " + props.getMaxMemCapacity()
                    + "  is the absolute maximum allowed.");


            return true;
        } else {
            log.info(getLogPreString() + " | isRecordsInStackMaximum --- We have "
                    + "reached MAX RECORDS in Stack - " + recordsInStack
                    + ", Resetting the bucket...");
            return false;
        }



    }

    public void failSafeResultApiInvokation(Map parameters, int resultStatus, String resultData, int transactionID) {
        this.resultApiFailureHandler.PersistFailedInvokation(parameters, resultStatus, resultData, transactionID);
    }

    /**
     *
     * @param task
     */
    public void executeTask(Runnable task) {

        try {
            threadPool.runTask(task);
        } catch (IllegalStateException is) {
            log.error(getLogPreString() + " | executeTask --- "
                    + "Failed to add this job to Bucket. "
                    + "Error: " + is.getMessage(), is);
        }
    }
}
