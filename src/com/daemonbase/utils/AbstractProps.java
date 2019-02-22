package com.cellulant.utils;

import com.cellulant.db.DATABASE;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

/**
 * Loads system properties from a file.
 *
 * @author <a href="brian.ngure@cellulant.com">Brian Ngure</a>
 * @author <a href="peter.juma@cellulant.com">Peter Bwire</a>
 */
public abstract class AbstractProps {

    Properties props;
    /**
     * The processed status code.
     */
    private int processedStatus;
    /**
     * The unprocessed status code.
     */
    private int unprocessedStatus;
    /**
     * The failed status code.
     */
    private int failedStatus;
    /**
     * The run id on startup.
     */
    private int startupRunID;
    /**
     * Maximum possible value of the run id.
     */
    private int maxRunID;
    /**
     * Minimum possible value of the run id.
     */
    private int minRunID;
    /**
     * Bucket size. This is the maximum size of records processed in a single
     * run.
     */
    private int bucketSize;
    /**
     * Sleep time in seconds.
     */
    private int sleepTime;
    /**
     * No of threads that will be created in the thread pool to process
     * payments.
     */
    private int numOfChildren;
    /**
     * ??????.
     */
    private int maxMemCapacity;
    /**
     * Maximum number of times to retry sending a payment.
     */
    private int maxSendRetries;
    /**
     * Info log level. Default = INFO.
     */
    private String infoLogLevel = "INFO";
    /**
     * Error log level. Default = FATAL.
     */
    private String errorLogLevel = "FATAL";
    /**
     * Info log file name.
     */
    private String infoLogFile;
    /**
     * Error log file name.
     */
    private String errorLogFile;
    /**
     * Maximum size of log file .
     */
    private String maxLogFileSize;
    /**
     * Maximum number of log files .
     */
    private int maxLogFileNumber = 10;
    /**
     * Database connection pool name.
     */
    private String dbPoolName;
    /**
     * Database connection pool size.
     */
    private int dbPoolSize;
    /**
     * Database user name.
     */
    private String dbUserName;
    /**
     * Database nodeSystemPassword.
     */
    private String dbPassword;
    /**
     * Database host.
     */
    private String dbHost;
    /**
     * Database port.
     */
    private String dbPort;
    /**
     * Database name.
     */
    private String dbName;
    /**
     * Type of database.
     */
    private DATABASE.DATABASETYPE dbType;
    /**
     * Type of database.
     */
    private String resultsApiSqliteDB;
    /**
     * Formula for getting time for future resending.
     */
    private String nextEarliestTimeToResend;
    /**
     * A list of any errors that occurred while loading the properties.
     */
    protected List<String> loadErrors;
    
    protected String profile;
    
    /**
     * The unit of time used to check validity(not expired) of a transaction
     */
    private String expirtyTimeUnit;

     /**
     * The value of time used to check validity(not expired) of a transaction
     */
    private int expirtyTimeValue;
    
    /**
     * Ideally if any arguments are added. args[0] = profile eg.
     * development,production ...
     *
     * Constructor.
     */
    public AbstractProps(String daemonPropertiesFile, String[] args) {
        loadErrors = new ArrayList<String>(0);

        String propsFile = daemonPropertiesFile;
        if (null != args && args.length > 0) {
            profile = args[0];
            propsFile = String.format(daemonPropertiesFile, profile);
        }
        
        if (null != args && args.length > 1) {
            propsFile =  args[1]+"/"+propsFile;
        }
        
        

        loadProperties(propsFile);

    }

    /**
     * Load system properties.
     *
     * @param propsFile the system properties xml file
     */
    private void loadProperties(final String propsFile) {

        InputStream propsStream = null;


        /**
         * Extract the values from the configuration file
         */
        try {
            props = new Properties();

            propsStream = new FileInputStream(propsFile);
            props.loadFromXML(propsStream);



            // ******** Get the logging configurations




            //Sleep Time

            sleepTime = readIntegerProp("SLEEP_TIME");

            //Bucket Size
            bucketSize = readIntegerProp("BUCKET_SIZE");


            //Number of children
            numOfChildren = readIntegerProp("NUM_OF_CHILDREN");


            //Current run at startup
            startupRunID = readIntegerProp("CURRENT_RUN_AT_STARTUP");


            //Maximum Run ID
            maxRunID = readIntegerProp("MAX_RUN_ID");


            //Minimum Run ID
            minRunID = readIntegerProp("MIN_RUN_ID");


            //Processed Status
            processedStatus = readIntegerProp("PROCESSED_STATUS");


            //Un Processed Status ID
            unprocessedStatus = readIntegerProp("UNPROCESSED_STATUS");


            //Failed Status ID
            failedStatus = readIntegerProp("FAILED_STATUS");


            //Maximum Memory Capacity
            maxMemCapacity = readIntegerProp("MAX_MEM_CAPACITY");


            //Maximum Number of Send Retries
            maxSendRetries = readIntegerProp("MAX_NUMBER_OF_SENDS");



            nextEarliestTimeToResend = readStringProp("NEXT_EARLIEST_TIME_TO_RESEND");



            if (nextEarliestTimeToResend.contains("|")) {

                //
                String[] resend = nextEarliestTimeToResend.split("\\|");
                nextEarliestTimeToResend = " adddate(now(), interval + ( %s ) minute) ";

                nextEarliestTimeToResend = String.format(nextEarliestTimeToResend,
                        " if( numberOfsends < 3 , ".concat(resend[0])
                        + " , if(numberOfsends < 6 , ".concat(resend[1])
                        + ", ".concat(resend[2]) + "))");


            } else {
                nextEarliestTimeToResend = " now()";

            }






            // ******** Get the logging configurations

            //Info Log File
            infoLogFile = readStringProp("INFO_LOG_FILE");


            //Error Log File
            errorLogFile = readStringProp("ERROR_LOG_FILE");


            infoLogLevel = readStringProp("INFO_LOG_LEVEL");


            //Info Log Level
            errorLogLevel = readStringProp("ERROR_LOG_LEVEL");



            //Maximum Log File Size
            maxLogFileSize = readStringProp("MAX_LOGFILE_SIZE");


            //Maximum Log File Number
            maxLogFileNumber = readIntegerProp("MAX_LOGFILE_NUMBER");


            // ******** Database configuration settings

            //Database pool name
            dbPoolName = readStringProp("DB_POOL_NAME");


            //Database pool size.
            dbPoolSize = readIntegerProp("DB_POOL_SIZE");

            dbUserName = readStringProp("DB_USER_NAME");



            //Database nodeSystemPassword
            dbPassword = readStringProp("DB_PASSWORD");


            //Database Host
            dbHost = readStringProp("DB_HOST");


            //Database Port
            dbPort = readStringProp("DB_PORT");


            //Database Name
            dbName = readStringProp("DB_NAME");

            //Database Type
            String dbTypeString = readOptionalStringProp("DB_TYPE", "mysql");

            if(dbTypeString.equalsIgnoreCase("mysql") || dbTypeString.isEmpty()){
                dbType = DATABASE.DATABASETYPE.MYSQL;
            }else if(dbTypeString.equalsIgnoreCase("oracle") ){
                dbType = DATABASE.DATABASETYPE.ORACLE;            
            }else if(dbTypeString.equalsIgnoreCase("db2") ){
                dbType = DATABASE.DATABASETYPE.DB2;            
            }else if(dbTypeString.equalsIgnoreCase("postgresql") ){
                dbType = DATABASE.DATABASETYPE.POSTGRESS;
            }else if(dbTypeString.equalsIgnoreCase("sybase") ){
                dbType = DATABASE.DATABASETYPE.SYBASE;
            }


            resultsApiSqliteDB = readOptionalStringProp("RESULTS_API_SQLITE_DB_LOCATION","/tmp/sqlite/"+profile+"/resultsApi.db");
             
            expirtyTimeUnit = readStringProp("EXPIRY_TIME_UNIT");
            
            expirtyTimeValue = readIntegerProp("EXPIRY_TIME_VALUE");
            
            //Loading of Extra properties by client.
            loadExtraProperties(props);


        } catch (FileNotFoundException ne) {

            System.err.println("Exiting. Could not find the properties file: "
                    + ne.getMessage());


        } catch (IOException ioe) {

            System.err.println("Exiting. Failed to load system properties: "
                    + ioe.getMessage());


        } catch (Exception e) {

            System.err.println("Exiting. Serious errors occuring . "
                    + e.getMessage());

            System.err.println(e);

        } finally {

            try {
                propsStream.close();
            } catch (IOException ex) {
                System.err.println("Failed to close the properties file: "
                        + ex.getMessage());
            }

        }
    }

    protected abstract void loadExtraProperties(Properties props) throws NumberFormatException;

    public String readOptionalStringProp(String key, String defaultKey) {

        String propValue = props.getProperty(key);
        if (propValue == null) {
            propValue = defaultKey;
        }

        return propValue;
    }

    public String readStringProp(String key) {

        String propValue = props.getProperty(key);
        if (propValue == null) {
            loadErrors.add("ERROR ON : " + key + "   Value is not set or is missing. ");
        }

        return propValue;
    }

    public long readLongProp(String key) {
        long propValue = 0;
        String tmpHolder = props.getProperty(key, "");
        if (!tmpHolder.isEmpty()) {
            try {
                propValue = Long.parseLong(tmpHolder);
            } catch (NumberFormatException ne) {
                loadErrors.add("ERROR ON : " + key + "   Value is not a double figure. " + ne.getMessage());
            }
        } else {
            loadErrors.add("ERROR ON : " + key + "   Value is not set or is missing. ");

        }
        return propValue;
    }

    public int readIntegerProp(String key) {
        int propValue = 0;
        String tmpHolder = props.getProperty(key, "");
        if (!tmpHolder.isEmpty()) {
            try {
                propValue = Integer.parseInt(tmpHolder);
            } catch (NumberFormatException ne) {
                loadErrors.add("ERROR ON : " + key + "   Value is not an integer figure. " + ne.getMessage());
            }


        } else {
            loadErrors.add("ERROR ON : " + key + "   Value is not set or is missing. ");

        }

        return propValue;
    }

    public double readDoubleProp(String key) {

        double propValue = 0;
        String tmpHolder = props.getProperty(key, "");
        if (!tmpHolder.isEmpty()) {

            try {
                propValue = Double.parseDouble(tmpHolder);
            } catch (NumberFormatException ne) {
                loadErrors.add("ERROR ON : " + key + "   Value is not a double figure. " + ne.getMessage());
            }


        } else {
            loadErrors.add("ERROR ON : " + key + "   Value is not set or is missing. ");

        }
        return propValue;


    }

    public boolean readBooleanProp(String key) {

        boolean booleanValue = false;
        String tmpHolder = props.getProperty(key);
        if (null != tmpHolder) {

            booleanValue = ("ON".equalsIgnoreCase(tmpHolder.trim())
                    || "1".equalsIgnoreCase(tmpHolder.trim())
                    || "TRUE".equalsIgnoreCase(tmpHolder.trim()));

        } else {
            loadErrors.add("ERROR ON : " + key + "   Value is not set or is missing. ");

        }
        return booleanValue;

    }

    /**
     * The processed status code.
     *
     * @return The processed status ID
     */
    public int getProcessedStatus() {
        return processedStatus;
    }

    /**
     * The unprocessed status code.
     *
     * @return The unprocessed status ID
     */
    public int getUnprocessedStatus() {
        return unprocessedStatus;
    }

    /**
     * The failed status code.
     *
     * @return The failed status
     */
    public int getFailedStatus() {
        return failedStatus;
    }

    /**
     * The run id on startup.
     *
     * @return The startup run ID
     */
    public int getStartupRunID() {
        return startupRunID;
    }

    /**
     * Maximum possible value of the run id.
     *
     * @return The maximum run ID
     */
    public int getMaxRunID() {
        return maxRunID;
    }

    /**
     * Minimum possible value of the run id.
     *
     * @return The minimum run ID
     */
    public int getMinRunID() {
        return minRunID;
    }

    /**
     * Bucket size. This is the maximum size of records processed in a single
     * run.
     *
     * @return The bucket size
     */
    public int getBucketSize() {
        return bucketSize;
    }

    /**
     * Sleep time in seconds.
     *
     * @return The sleep time
     */
    public int getSleepTime() {
        return sleepTime;
    }

    /**
     * No of threads that will be created in the thread pool to process
     * payments.
     *
     * @return The Number of Children
     */
    public int getNumOfChildren() {
        return numOfChildren;
    }

    /**
     * Gets the maximum memory capacity.
     *
     * @return the maxMemCapacity
     */
    public int getMaxMemCapacity() {
        return maxMemCapacity;
    }

    /**
     * Maximum number of times to retry sending a payment.
     *
     * @return The Max Send Retries
     */
    public int getMaxSendRetries() {
        return maxSendRetries;
    }

    /**
     * Info log level. Default = INFO.
     *
     * @return the info log level
     */
    public String getInfoLogLevel() {
        return infoLogLevel;
    }

    /**
     * Error log level. Default = FATAL.
     *
     * @return The error log level
     */
    public String getErrorLogLevel() {
        return errorLogLevel;
    }

    /**
     * Info log file name.
     *
     * @return The info log file
     */
    public String getInfoLogFile() {
        return infoLogFile;
    }

    /**
     * Error log file name.
     *
     * @return The error log file
     */
    public String getErrorLogFile() {
        return errorLogFile;
    }

    /**
     *
     * @return The maximum log file size.
     */
    public String getMaxLogFileSize() {
        return maxLogFileSize;
    }

    /**
     *
     * @return The maximum number of log files.
     */
    public int getMaxLogFileNumber() {
        return maxLogFileNumber;
    }

    /**
     * Contains the name of the database pool.
     *
     * @return The name of the database pool
     */
    public String getDbPoolName() {
        return dbPoolName;
    }

    /**
     * Contains the size of the database pool.
     *
     * @return The size of the database pool
     */
    public int getDbPoolSize() {
        return dbPoolSize;
    }

    /**
     * Contains the name of the database user.
     *
     * @return The name of the database user
     */
    public String getDbUserName() {
        return dbUserName;
    }

    /**
     * Contains the name of the database nodeSystemPassword.
     *
     * @return The name of the database nodeSystemPassword
     */
    public String getDbPassword() {
        return dbPassword;
    }

    /**
     * Contains the name of the database host.
     *
     * @return The name of the database host
     */
    public String getDbHost() {
        return dbHost;
    }

    /**
     * Contains the name of the database port.
     *
     * @return The name of the database port
     */
    public String getDbPort() {
        return dbPort;
    }

    /**
     * Contains the name of the database.
     *
     * @return The name of the database
     */
    public String getDbName() {
        return dbName;
    }

    /**
     * Type of database that is configured to work with the daemon.
     * @return 
     */
    public DATABASE.DATABASETYPE getDbType() {
        return dbType;
    }

    
    
    /**
     * A list of any errors that occurred while loading the properties.
     *
     * @return The list of load errors
     */
    public List<String> getLoadErrors() {
        return Collections.unmodifiableList(loadErrors);
    }

    public String getNextEarliestTimeToResend() {
        return nextEarliestTimeToResend;
    }

    public String getResultsApiSqliteDB() {
        return resultsApiSqliteDB;
    }

    public String getExpiryTimeUnit() {
        return expirtyTimeUnit;
    }

    public int getExpiryTimeValue() {
        return expirtyTimeValue;
    }
    
    
    
}
