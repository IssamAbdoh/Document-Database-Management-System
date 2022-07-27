package slave.sharedpackages.logging;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LoggingService
{
    private LoggingService()
    {
    }
    
    private static final Logger logger = LoggerFactory.getLogger( LoggingService.class );
    
    public static void logInfo(String message)
    {
        logger.info( "{} {} {}" , System.currentTimeMillis() , Thread.currentThread().getName() , message );
    }
    
    public static void logError(String message)
    {
        logger.error( "{} {} {}" , System.currentTimeMillis() , Thread.currentThread().getName() , message );
    }
    
    public static void logDebug(String message)
    {
        logger.debug( "{} {} {}" , System.currentTimeMillis() , Thread.currentThread().getName() , message );
    }
    
    public static void logWarn(String message)
    {
        logger.warn( "{} {} {}" , System.currentTimeMillis() , Thread.currentThread().getName() , message );
    }
    
    public static void logTrace(String message)
    {
        logger.trace( "{} {} {}" , System.currentTimeMillis() , Thread.currentThread().getName() , message );
    }
}
