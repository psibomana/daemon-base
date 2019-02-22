package com.cellulant.utils;

import java.io.IOException;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.apache.log4j.RollingFileAppender;

/**
 * Initialises the log files.
 *
 * @author <a href="brian.ngure@cellulant.com">Brian Ngure</a>
 * 
 */
@SuppressWarnings("FinalClass")
public final class Logging {
   
    
   
    /**
     * Info log.
     */
    private static Logger infoLog;
    /**
     * Error log.
     */
    private static Logger errorLog;
    /**
     * Loaded system properties.
     */
    private AbstractProps props;

    /**
     * Constructor.
     *
     * @param props passed in loaded system properties
     */
    public Logging(final AbstractProps props) {
        this.props = props;
        initializeLoggers();
    }

    /**
     * Initialise the log managers.
     */
    @SuppressWarnings({"CallToThreadDumpStack", "UseOfSystemOutOrSystemErr"})
    private void initializeLoggers() {
        infoLog = Logger.getLogger("infoLog");
        errorLog = Logger.getLogger("errorLog");

        PatternLayout layout = new PatternLayout();
        layout.setConversionPattern("%d{yyyy MMM dd HH:mm:ss,SSS}: %p : %m%n");

        try {
            RollingFileAppender rfaINFO_LOG = new RollingFileAppender(layout,
                    props.getInfoLogFile(), true);
            rfaINFO_LOG.setMaxFileSize(this.props.getMaxLogFileSize());
            rfaINFO_LOG.setMaxBackupIndex(this.props.getMaxLogFileNumber());

            RollingFileAppender rfaERROR_LOG = new RollingFileAppender(layout,
                    props.getErrorLogFile(), true);
            rfaERROR_LOG.setMaxFileSize(this.props.getMaxLogFileSize());
            rfaERROR_LOG.setMaxBackupIndex(this.props.getMaxLogFileNumber());

            infoLog.addAppender(rfaINFO_LOG);
            errorLog.addAppender(rfaERROR_LOG);
        } catch (IOException ex) {
            System.err.println("Failed to initialize loggers... EXITING: "
                    + ex.getMessage());
            ex.printStackTrace();
            System.exit(1);
        }

        infoLog.setLevel(Level.toLevel(props.getInfoLogLevel()));
        errorLog.setLevel(Level.toLevel(props.getErrorLogLevel()));

        info("Just finished initializing Loggers...");
    }

    /**
     * Log info messages.
     *
     * @param message the message content
     */
    public void info(final String message) {
        infoLog.info(message);
    }
    
    
    /**
     * Log debug messages.
     *
     * @param message the message content
     */
    public void debug(final String message) {
        infoLog.debug(message);
    }
    

    /**
     * Log error messages.
     *
     * @param message the message content
     */
    public void error(final String message) {
        errorLog.error(message);
    }
    
    
    /**
     * Log error messages.
     *
     * @param message the message content
     * @param throwable.
     */
    public void error(final String message,final Throwable t) {
        errorLog.error(message,t);
    }

    /**
     * Log fatal error messages.
     *
     * @param message the message content
     */
    public void fatal(final String message) {
        errorLog.fatal(message);
    }
    
       /**
     * Log fatal error messages.
     *
     * @param message the message content
     * @param throwable.
     */
    public void fatal(final String message,final Throwable t) {
        errorLog.error(message,t);
    }
}
