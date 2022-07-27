package scale.control;

import org.springframework.retry.annotation.Backoff;
import org.springframework.retry.annotation.Recover;
import org.springframework.retry.annotation.Retryable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import scale.logging.LoggingService;

import java.io.IOException;

@RestController
@RequestMapping (value = "/scale")
public class ScaleController
{
    private static int id = 2616;
    
    @PostMapping (value = "/addNewSlave")
    @Retryable (value = { Exception.class }, maxAttempts = 4, backoff = @Backoff (delay = 1000))
    public void addNewSlave()
    {
        try
        {
            //run docker image demo_slave
            String command =
                    "docker run demo_slave1 " + " -p " + id + ":" + id + " --network=group1 " + " --rm " + " --name=slave" + id + " -v " + "./slave"
                            + ":/usr/src/myapp/slave ";
            //specify the entrypoint of the container
            command += " --entrypoint java -jar target/slave-0.0.1-SNAPSHOT.jar --server.port=" + (id++);
            //command += " demo_slave1";
            Process p = Runtime.getRuntime().exec( command );
            System.out.println( "Running command: " + command );
        }
        catch(IOException e)
        {
            throw new RuntimeException( e );
        }
    }
    
    @Recover
    public void recover(Exception e)
    {
        LoggingService.logError(
                "*************************************************************************************************************************" );
        LoggingService.logError( "All retries completed, so Fallback method called!!!" );
        LoggingService.logError( "Recovering from exception" );
        LoggingService.logError( e.getMessage() );
        LoggingService.logError(
                "*************************************************************************************************************************" );
    }
}
