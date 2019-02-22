package com.cellulant.utils;

import com.cellulant.db.DATABASE;
import com.google.gson.Gson;
import java.io.File;
import java.io.FileNotFoundException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 *
 * @author Peter J. Bwire <peter.juma@cellulant.com>
 */
public class ResultApiFailureHandler {

    final List<List<Object>> listOfQueries;
    /**
     * The database store for sqlite db.
     */
    DATABASE db;
    /**
     * System properties class instance.
     */
    private AbstractProps props;
    /**
     * Log class instance.
     */
    private Logging log;
    private String daemonIdentity;
    private Timer periodicDbLogger;

    /**
     * @param props the loaded system properties
     * @param logging the daemons state logger.
     */
    public ResultApiFailureHandler(final AbstractProps props, final Logging logging, final String daemonIdentity) {

        this.listOfQueries = new CopyOnWriteArrayList<List<Object>>();
        this.daemonIdentity = daemonIdentity;
        this.props = props;
        log = logging;
        
        createResultApiDatabase();
        periodicDbLogger = new Timer();
        periodicDbLogger.scheduleAtFixedRate(new SlowTaskResultProcessor(), 0, 20000);
        
    }

    private boolean setUpDatabase() {
        try {

            if (db == null ) {
                db = new DATABASE(props.getResultsApiSqliteDB(), "ResultsApiLogger", 100);
                log.info(getLogPreString() + " | setUpDatabase successfully setup database.");

            } else {
                log.info(getLogPreString() + " | setUpDatabase was already successfully setup hence reusing connections...");
            }
            
            if (! new File(props.getResultsApiSqliteDB()).exists())
            {
                throw new FileNotFoundException(" Our Database is missing.");
            }
            return true;
        } catch (Exception ex) {
            log.error(" Problems instantiating sqlite Database : ", ex);
            
            
            log.info(getLogPreString() + " | setUpDatabase creating a fresh installation of database hence retrying setup.");

            return createResultApiDatabase();

        }
    }

    public void PersistFailedInvokation(Map parameters, int resultStatus, String resultData, int transactionID) {


        log.info(getLogPreString()
                + "Checking for partially processed records...");

         //Set up parameters :
                Gson gson = new Gson();
                String payload = gson.toJson(parameters);
               List<Object> objects = new ArrayList<Object>();
               objects.add(payload);
               objects.add(resultStatus);
               objects.add(resultData);
               objects.add(daemonIdentity);
               objects.add(transactionID);
             
               this.listOfQueries.add(objects);
      
    }

    public final boolean createResultApiDatabase() {

        String createQuery = "CREATE TABLE IF NOT EXISTS requests (  requestID integer primary key,  clientTransactionID int,"
                + "  payload text,  resultData text,  resultStatus int,  numberOfSends int not null default 0,"
                + "  processed int not null default 0,  processedStatus int,  insertedBy text,  dateCreated datetime,"
                + "  updatedBy text,  dateModified CURRENT_TIMESTAMP,  UNIQUE (clientTransactionID,insertedBy));";


        PreparedStatement stmt = null;
        Connection conn = null;

        try {


            File f = new File(props.getResultsApiSqliteDB());

            if (!f.exists()) {
                f.createNewFile();
        
            }

                f.setWritable(true);
                f.setReadable(true);
                f.setExecutable(true);

            db = new DATABASE(props.getResultsApiSqliteDB(), "ResultsApiLogger", 100);
            log.info(getLogPreString() + " | setUpDatabase successfully setup database.");

            conn = db.getConnection();
            stmt = conn.prepareStatement(createQuery);


            int st = stmt.executeUpdate();
            log.info(getLogPreString() + "  createResultApiDatabase  Successfulle created a new result api db ...");

            return true;

        } catch (SQLException e) {
            log.error(getLogPreString() + "  createResultApiDatabase "
                    + "--- Failed to insert records. The query - " + createQuery
                    + ". Reason: ", e);

        } catch (Exception e) {
            log.error(getLogPreString() + "  createResultApiDatabase --- Problems with result api - "
                    + ". Reason: ", e);

        } finally {
            if (stmt != null) {
                try {
                    stmt.close();
                } catch (SQLException sqlex) {
                    log.error(getLogPreString()
                            + "createResultApiDatabase --- "
                            + "Failed to close Statement object. Reason: "
                            + sqlex.getMessage());
                }
            }

            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException sqle) {
                    log.error(getLogPreString()
                            + "createResultApiDatabase --- "
                            + "Failed to close connection object. Reason: "
                            + sqle.getMessage());
                }
            }
        }

        return false;
    }
    
    
    
    
    
    class SlowTaskResultProcessor extends TimerTask{

        @Override
        public void run() {
           try{
           
               List<List<Object>> shortList = new ArrayList<List<Object>>();
               shortList.addAll(listOfQueries);
               
               synchronized (listOfQueries) {listOfQueries.clear();}
               
               for (List<Object> object : shortList) {                   
                   StoreInDB(object);
               }
               
               
           }catch(Exception e){
                log.error(getLogPreString() + "  SlowTaskResultProcessor "
                        + "--- Great details and Problems, srious ones... The query - " 
                        + ". Reason: ", e);

           }
        }
        
        
        private void StoreInDB(List<Object> params){
        
              if (setUpDatabase()) {

            PreparedStatement stmt = null;
            Connection conn = null;
            String insertQuery = "INSERT INTO requests (payload, resultStatus, resultData, insertedBy, clientTransactionID,dateCreated ) VALUES (?,?,?,?, ?,datetime('now'))  ";

            try {
                conn = db.getConnection();
                stmt = conn.prepareStatement(insertQuery);

               


                stmt.setString(1, (String) params.get(0));
                stmt.setInt(2, (Integer) params.get(1));
                stmt.setString(3, (String) params.get(2));
                stmt.setString(4, (String) params.get(3));
                stmt.setInt(5, (Integer) params.get(4));
                
                
                

                int st = stmt.executeUpdate();
                log.info(getLogPreString()
                        + "Successfull queue for failed result api invokation ...");



            } catch (SQLException e) {
                log.error(getLogPreString() + "  PersistFailedInvokation "
                        + "--- Failed to insert records. The query - " + insertQuery
                        + ". Reason: ", e);

            } finally {
                if (stmt != null) {
                    try {
                        stmt.close();
                    } catch (SQLException sqlex) {
                        log.error(getLogPreString()
                                + "PersistFailedInvokation --- "
                                + "Failed to close Statement object. Reason: "
                                + sqlex.getMessage());
                    }
                }

                if (conn != null) {
                    try {
                        conn.close();
                    } catch (SQLException sqle) {
                        log.error(getLogPreString()
                                + "PersistFailedInvokation --- "
                                + "Failed to close connection object. Reason: "
                                + sqle.getMessage());
                    }
                }
            }

        }

        }
    
    }
    
    

    /**
     * Prepended text added to each log message.
     *
     * @return "ResultApiFailureHandler | "
     */
    public final String getLogPreString() {
        return this.getClass().getSimpleName() + " | ";
    }
}
