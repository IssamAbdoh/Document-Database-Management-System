package master.control;


import master.sharedpackages.logging.LoggingService;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.retry.annotation.Backoff;
import org.springframework.retry.annotation.Recover;
import org.springframework.retry.annotation.Retryable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.io.UnsupportedEncodingException;

@RestController
@RequestMapping (value = "/master/scale")
public class ScaleController
{
    @PostMapping (value = "/addNewSlave")
    @Retryable (value = { Exception.class }, maxAttempts = 4, backoff = @Backoff (delay = 1000))
    public void addNewSlave()
    {
        //send post request localhost port 1234 to add new slave
        String url = "http://localhost:1234/scale/addNewSlave";
        url = "http://host.docker.internal:1234/scale/addNewSlave";
        sendPostRequest( url , "" );
    }
    
    private static void sendPostRequest(String url , String json)
    {
        CloseableHttpClient client = HttpClientBuilder.create().build();
        HttpPost post = new HttpPost( url );
        StringEntity entity;
        try
        {
            entity = new StringEntity( json );
        }
        catch(UnsupportedEncodingException e)
        {
            throw new RuntimeException( e );
        }
        entity.setContentType( "application/json" );
        post.setEntity( entity );
        try
        {
            HttpResponse response = client.execute( post );
            LoggingService.logInfo( "sendPostRequest to " + url + "Response Code : " + response );
            ResponseEntity.status( HttpStatus.OK ).body( "SUCCESS BS upload" );
        }
        catch(Exception e)
        {
            LoggingService.logError( e.getMessage() );
            ResponseEntity.status( HttpStatus.EXPECTATION_FAILED ).body( "FAIL BS upload" );
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
