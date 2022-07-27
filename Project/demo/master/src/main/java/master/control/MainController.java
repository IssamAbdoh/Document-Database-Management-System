package master.control;

import master.MasterNode;
import master.PATHS;
import master.indexing.IndexGeneratingService;
import master.sharedpackages.indexing.Index;
import master.sharedpackages.logging.LoggingService;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.entity.mime.HttpMultipartMode;
import org.apache.http.entity.mime.MultipartEntityBuilder;
import org.apache.http.entity.mime.content.FileBody;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cloud.client.ServiceInstance;
import org.springframework.cloud.client.discovery.DiscoveryClient;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.retry.annotation.Backoff;
import org.springframework.retry.annotation.Recover;
import org.springframework.retry.annotation.Retryable;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.client.RestTemplate;
import org.zeroturnaround.zip.ZipUtil;

import java.io.File;
import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.List;

@RestController
@RequestMapping (value = "/master")
public class MainController
{
    //send data updates always
    //send schemas updates
    //ask slaves to flush indexes of a certain collection
    //need to flush its own indexes when updating data
    //send indexes when asked to
    
    private final MasterNode master_node;
    private final RestTemplate rest_template;
    
    final DiscoveryClient discovery_client;
    IndexGeneratingService index_generating_service;
    
    @Autowired
    public MainController(MasterNode master_node , RestTemplate rest_template , DiscoveryClient discovery_client ,
                          IndexGeneratingService index_generating_service)
    {
        this.rest_template = rest_template;
        this.master_node = master_node;
        this.discovery_client = discovery_client;
        this.index_generating_service = index_generating_service;
    }
    
    //@RequestBody is used to get the data from the request body
    //@PathVariable is used to get the data from the path
    //@RequestParam is used to get the data from the query string
    
    ////creating
    
    @PostMapping (value = "/{database}")
    @Retryable (value = { Exception.class }, maxAttempts = 4, backoff = @Backoff (delay = 1000))
    public void createDatabase(@PathVariable ("database") String database)
    {
        master_node.createDatabase( PATHS.DATABASES_PATH , database );
        propagateData();
        flushSlaveIndex( database );
        flushMasterIndex( database );
    }
    
    @PostMapping (value = "/{database}/{collection}")
    @Retryable (value = { Exception.class }, maxAttempts = 4, backoff = @Backoff (delay = 1000))
    public void createCollection(@PathVariable ("database") String database , @PathVariable ("collection") String collection)
    {
        master_node.createCollection( database , collection );
        propagateData();
        flushSlaveIndex( database , collection );
        flushMasterIndex( database , collection );
    }
    
    ////inserting
    
    @PostMapping (value = "/{database}/{collection}/insertDocument")
    @Retryable (value = { Exception.class }, maxAttempts = 4, backoff = @Backoff (delay = 1000))
    public void insertDocument(@RequestBody String request_body , @PathVariable ("database") String database ,
                               @PathVariable ("collection") String collection)
    {
        JSONObject request_body_json = new JSONObject( request_body );
        master_node.insertDocument( database , collection , request_body_json.getJSONObject( "json_object" ) );
        propagateData();
        flushSlaveIndex( database , collection );
        flushMasterIndex( database , collection );
    }
    
    ////update the database
    
    @PutMapping (value = "/{database}/{collection}/{id}")
    @Retryable (value = { Exception.class }, maxAttempts = 4, backoff = @Backoff (delay = 1000))
    public void updateDocument(@RequestBody String request_body , @PathVariable ("database") String database ,
                               @PathVariable ("collection") String collection , @PathVariable ("id") long id)
    {
        JSONObject request_body_json = new JSONObject( request_body );
        master_node.updateDocument( database , collection , id , request_body_json.getJSONObject( "updated_json_object" ) );
        propagateData();
        flushSlaveIndex( database , collection );
        flushMasterIndex( database , collection );
    }
    
    @PutMapping (value = "/{database}")
    @Retryable (value = { Exception.class }, maxAttempts = 4, backoff = @Backoff (delay = 1000))
    public void updateDatabaseName(@RequestBody String request_body , @PathVariable ("database") String database)
    {
        JSONObject request_body_json = new JSONObject( request_body );
        master_node.updateDatabaseName( database , request_body_json.get( "new_database_name" ).toString() );
        propagateData();
        flushSlaveIndex( database );
        flushMasterIndex( database );
    }
    
    @PutMapping (value = "/{database}/{collection}")
    @Retryable (value = { Exception.class }, maxAttempts = 4, backoff = @Backoff (delay = 1000))
    public void updateCollectionName(@RequestBody String request_body , @PathVariable ("database") String database ,
                                     @PathVariable ("collection") String collection)
    {
        JSONObject request_body_json = new JSONObject( request_body );
        master_node.updateCollectionName( database , collection , request_body_json.get( "new_collection_name" ).toString() );
        propagateData();
        flushSlaveIndex( database , collection );
        flushMasterIndex( database , collection );
    }
    
    ////delete from the database
    
    @DeleteMapping (value = "/{database}")
    @Retryable (value = { Exception.class }, maxAttempts = 4, backoff = @Backoff (delay = 1000))
    public void deleteDatabase(@PathVariable ("database") String database)
    {
        master_node.deleteDatabase( database );
        propagateData();
        flushSlaveIndex( database );
        flushMasterIndex( database );
    }
    
    @DeleteMapping (value = "/{database}/{collection}")
    @Retryable (value = { Exception.class }, maxAttempts = 4, backoff = @Backoff (delay = 1000))
    public void deleteCollection(@PathVariable ("database") String database , @PathVariable ("collection") String collection)
    {
        master_node.deleteCollection( database , collection );
        propagateData();
        flushSlaveIndex( database , collection );
        flushMasterIndex( database , collection );
    }
    
    @DeleteMapping (value = "/{database}/{collection}/{id}")
    @Retryable (value = { Exception.class }, maxAttempts = 4, backoff = @Backoff (delay = 1000))
    public void deleteDocument(@PathVariable ("database") String database , @PathVariable ("collection") String collection ,
                               @PathVariable ("id") long document_id)
    {
        master_node.deleteDocument( database , collection , document_id );
        propagateData();
        flushSlaveIndex( database , collection );
        flushMasterIndex( database , collection );
    }
    
    @DeleteMapping (value = "/{database}/{collection}/{id}/{property}")
    @Retryable (value = { Exception.class }, maxAttempts = 4, backoff = @Backoff (delay = 1000))
    public void deleteField(@PathVariable ("database") String database , @PathVariable ("collection") String collection ,
                            @PathVariable ("id") long document_id , @PathVariable ("property") String property)
    {
        master_node.deleteProperty( database , collection , document_id , property );
        propagateData();
        flushSlaveIndex( database , collection );
        flushMasterIndex( database , collection );
    }
    
    ////export
    
    @PostMapping (value = "/export/{database}")
    @Retryable (value = { Exception.class }, maxAttempts = 4, backoff = @Backoff (delay = 1000))
    public void exportDatabase(@RequestBody String request_body , @PathVariable ("database") String database)
    {
        JSONObject request_body_json = new JSONObject( request_body );
        master_node.exportDatabase( database , request_body_json.get( "schema_name" ).toString() );
        propagateSchemas();
    }
    
    @PostMapping (value = "/exportLocally/{database}")
    @Retryable (value = { Exception.class }, maxAttempts = 4, backoff = @Backoff (delay = 1000))
    public void exportDatabaseLocally(@RequestBody String request_body , @PathVariable ("database") String database)
    {
        JSONObject request_body_json = new JSONObject( request_body );
        String schema_name = request_body_json.get( "schema_name" ).toString();
        String export_path = request_body_json.get( "export_path" ).toString();
        master_node.exportLocally( database , schema_name , export_path );
    }
    
    ////import a schema name
    @PostMapping (value = "/import")
    @Retryable (value = { Exception.class }, maxAttempts = 4, backoff = @Backoff (delay = 1000))
    public void importSchema(@RequestBody String request_body)
    {
        JSONObject request_body_json = new JSONObject( request_body );
        String schema_name = request_body_json.get( "schema_name" ).toString();
        master_node.importDatabase( schema_name );
        propagateData();
    }
    
    //import with a path
    @PostMapping (value = "/importLocally")
    @Retryable (value = { Exception.class }, maxAttempts = 4, backoff = @Backoff (delay = 1000))
    public void importSchemaLocally(@RequestBody String request_body)
    {
        JSONObject request_body_json = new JSONObject( request_body );
        String schema_path = request_body_json.get( "schema_path" ).toString();
        master_node.importLocally( schema_path );
        propagateData();
    }
    
    //post request to send a directory to the slave
    @PostMapping (value = "/propagateDataToAllSlaves")
    @Retryable (value = { Exception.class }, maxAttempts = 4, backoff = @Backoff (delay = 1000))
    public void propagateDataToAllSlaves()
    {
        propagateData();
    }
    
    private void propagate(String directory_name)
    {
        ZipUtil.pack( new File( PATHS.MASTER_PATH + "/" + directory_name + "/" ) , new File( PATHS.MASTER_PATH + "/" + directory_name + ".zip" ) );
        
        List<ServiceInstance> slaves = discovery_client.getInstances( "SLAVE" );
        slaves.forEach( s ->
        {
            try
            {
                String url = "http://" + s.getHost() + ":" + s.getPort() + "/slave/update" + StringUtils.capitalize( directory_name );
                LoggingService.logInfo( "propagate sending to " + url );
                sendFile( PATHS.MASTER_PATH + "/" + directory_name + ".zip" , url );
            }
            catch(Exception e)
            {
                e.printStackTrace();
            }
        } );
    }
    
    private void propagateData()
    {
        propagate( "data" );
    }
    
    private static void propagateDataToASlave(int id)
    {
        ZipUtil.pack( new File( PATHS.DATA_DIRECTORY_PATH ) , new File( PATHS.DATA_ZIP_PATH ) );
        sendFile( PATHS.DATA_ZIP_PATH , "http://localhost:" + id + "/slave/updateData" );
    }
    
    private static void sendFile(String file_name , String myFile_URL)
    {
        CloseableHttpClient client = HttpClientBuilder.create().build();
        
        HttpPost post = new HttpPost( myFile_URL );
        
        //put the directory in a variable
        File file = new File( file_name );
        
        FileBody fileBody = new FileBody( file , ContentType.DEFAULT_BINARY );
        MultipartEntityBuilder builder = MultipartEntityBuilder.create();
        builder.setMode( HttpMultipartMode.BROWSER_COMPATIBLE );
        builder.addPart( "file" , fileBody );
        HttpEntity entity = builder.build();
        post.setEntity( entity );
        
        try
        {
            HttpResponse response = client.execute( post );
            LoggingService.logInfo( "sendfile to " + myFile_URL + "Response Code : " + response );
            ResponseEntity.status( HttpStatus.OK ).body( "SUCCESS BS upload" );
        }
        catch(Exception e)
        {
            LoggingService.logError( e.getMessage() );
            ResponseEntity.status( HttpStatus.EXPECTATION_FAILED ).body( "FAIL BS upload" );
        }
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
    
    @PostMapping (value = "/propagateData/{slave_id}")
    @Retryable (value = { Exception.class }, maxAttempts = 4, backoff = @Backoff (delay = 1000))
    public void propagateDataToSlave(@PathVariable ("slave_id") int slave_id)
    {
        propagateDataToASlave( slave_id );
    }
    
    @PostMapping (value = "/propagateSchemasToSlaves")
    @Retryable (value = { Exception.class }, maxAttempts = 4, backoff = @Backoff (delay = 1000))
    public void propagateSchemasToAll()
    {
        propagateSchemas();
    }
    
    private void propagateSchemas()
    {
        propagate( "exported" );
    }
    
    //get collection index
    @GetMapping (value = "/{database}/{collection}/{property}/index")
    @Retryable (value = { Exception.class }, maxAttempts = 4, backoff = @Backoff (delay = 1000))
    public String getIndex(@PathVariable ("database") String database , @PathVariable ("collection") String collection ,
                           @PathVariable ("property") String property)
    {
        Index index = index_generating_service.getCollectionIndex( database , collection , property );
        LoggingService.logInfo( "getIndex " + database + " " + collection + " " + property + " " + index );
        return index.toString();
    }
    
    private void flushMasterIndex(String database_name)
    {
        index_generating_service.deleteDatabaseIndex( database_name );
    }
    
    private void flushMasterIndex(String database_name , String collection_name)
    {
        index_generating_service.deleteCollectionIndex( database_name , collection_name );
    }
    
    private void flushSlaveIndex(String database_name)
    {
        flushSlaveIndex( database_name , "" );
    }
    
    private void flushSlaveIndex(String database_name , String collection_name)
    {
        HashMap<String, String> data = new HashMap<>();
        data.put( "database" , database_name );
        data.put( "collection" , collection_name );
        List<ServiceInstance> instances = discovery_client.getInstances( "slave" );
        for(ServiceInstance instance : instances)
        {
            String port = String.valueOf( instance.getPort() );
            sendPostRequest( "http://localhost:" + port + "/slave/flushIndex" , new JSONObject( data ).toString() );
            LoggingService.logInfo( "flushed index of " + database_name + "/" + collection_name + "for slave " + port );
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
