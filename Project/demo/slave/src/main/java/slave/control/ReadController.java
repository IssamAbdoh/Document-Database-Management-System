package slave.control;

import org.json.JSONArray;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.retry.annotation.Backoff;
import org.springframework.retry.annotation.Recover;
import org.springframework.retry.annotation.Retryable;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.multipart.MultipartFile;
import org.zeroturnaround.zip.ZipUtil;
import slave.PATHS;
import slave.indexing.IndexStoringService;
import slave.sharedpackages.indexing.Index;
import slave.sharedpackages.logging.LoggingService;
import slave.sharedpackages.rwoperations.DatabaseReadService;
import slave.sharedpackages.rwoperations.DatabaseWriteService;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Objects;

@RestController
@RequestMapping (value = "/slave")
public class ReadController
{
    //should receive files and extract them
    //should receive reads
    //when receiving reads, should check if index is stored
    //if not, should ask master for index
    //when receiving updates for a collection, should check if index is stored and if yes it should flush it
    
    public RestTemplate rest_template;
    public IndexStoringService index_storing_service;
    
    @Autowired
    public ReadController(RestTemplate rest_template , IndexStoringService index_storing_service)
    {
        this.index_storing_service = index_storing_service;
        this.rest_template = rest_template;
    }
    
    @PostMapping ("/updateData")
    @Retryable (value = { Exception.class }, maxAttempts = 4, backoff = @Backoff (delay = 1000))
    public ResponseEntity<String> updateData(@RequestParam ("file") MultipartFile file)
    {
        alert();
        return updateFile( file , PATHS.DATA_ZIP_PATH , PATHS.DATA_DIRECTORY_PATH );
    }
    
    @PostMapping ("/updateExported")
    @Retryable (value = { Exception.class }, maxAttempts = 4, backoff = @Backoff (delay = 1000))
    public ResponseEntity<String> updateExported(@RequestParam ("file") MultipartFile file)
    {
        alert();
        return updateFile( file , PATHS.EXPORTED_ZIP_PATH , PATHS.EXPORT_DIRECTORY_PATH );
    }
    
    
    //post method to flush index
    @PostMapping ("/flushIndex")
    @Retryable (value = { Exception.class }, maxAttempts = 4, backoff = @Backoff (delay = 1000))
    public ResponseEntity flushIndex(@RequestBody HashMap<String, String> request_body)
    {
        alert();
        JSONObject request_body_json = new JSONObject( request_body );
        String database = request_body_json.getString( "database" );
        String collection = request_body_json.getString( "collection" );
        if(collection.equals( "" ))
        {
            index_storing_service.deleteDatabaseIndex( database );
        }
        else
        {
            index_storing_service.deleteCollectionIndex( database , collection );
        }
        
        LoggingService.logError( "Index flushed" + database + " " + collection );
        
        return new ResponseEntity( HttpStatus.CREATED );
    }
    
    
    @GetMapping (value = "/{database}/{collection}/document")//take id RequestParam
    @Retryable (value = { Exception.class }, maxAttempts = 4, backoff = @Backoff (delay = 1000))
    public String getDocument(@PathVariable ("database") String database , @PathVariable ("collection") String collection , @RequestParam String id)
    {
        alert();
        database = database.toLowerCase();
        collection = collection.toLowerCase();
        
        return getDocumentsFromIndex( database , collection , "id" , String.valueOf( id ) );
    }
    
    @GetMapping (value = "/{database}/{collection}/documents/{id}/{field}")
    @Retryable (value = { Exception.class }, maxAttempts = 4, backoff = @Backoff (delay = 1000))
    public String getField(@PathVariable ("database") String database , @PathVariable ("collection") String collection ,
                           @PathVariable ("id") long id , @PathVariable ("field") String field)
    {
        alert();
        database = database.toLowerCase();
        collection = collection.toLowerCase();
        field = field.toLowerCase();
        
        return getFieldFromIndex( database , collection , "id" , String.valueOf( id ) , field );
    }
    
    //    public static JSONArray getCollection(String database_name , String collection_name)
    @GetMapping (value = "/{database}/{collection}")
    @Retryable (value = { Exception.class }, maxAttempts = 4, backoff = @Backoff (delay = 1000))
    public String getCollection(@PathVariable ("database") String database , @PathVariable ("collection") String collection)
    {
        alert();
        
        database = database.toLowerCase();
        collection = collection.toLowerCase();
        
        JSONArray collection1 = DatabaseReadService.getCollection( PATHS.DATABASES_PATH , database , collection );
        return collection1.toString();
    }
    
    
    //    public static ArrayList<JSONArray> getDatabase(String database_name)
    @GetMapping (value = "/{database}")
    @Retryable (value = { Exception.class }, maxAttempts = 4, backoff = @Backoff (delay = 1000))
    public String getDatabase(@PathVariable ("database") String database)
    {
        alert();
        
        database = database.toLowerCase();
        
        return Objects.requireNonNull( DatabaseReadService.getDatabase( PATHS.DATABASES_PATH , database ) ).toString();
    }
    
    
    //    public static ArrayList<String> getAllCollectionNames(String database_name)
    @GetMapping (value = "/{database}/collectionsNames")
    @Retryable (value = { Exception.class }, maxAttempts = 4, backoff = @Backoff (delay = 1000))
    public String getAllCollectionNames(@PathVariable ("database") String database)
    {
        alert();
        
        database = database.toLowerCase();
        
        return Objects.requireNonNull( DatabaseReadService.getAllCollectionNames( PATHS.DATABASES_PATH , database ) ).toString();
    }
    
    
    @GetMapping (value = "/{database}/{collection}/documents")
    @Retryable (value = { Exception.class }, maxAttempts = 4, backoff = @Backoff (delay = 1000))
    public String getDocuments(@PathVariable ("database") String database , @PathVariable ("collection") String collection ,
                               @RequestParam String field , @RequestParam String value)
    {
        alert();
        
        database = database.toLowerCase();
        collection = collection.toLowerCase();
        field = field.toLowerCase();
        value = value.toLowerCase();
        
        return getDocumentsFromIndex( database , collection , field , value );
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
    
    private ResponseEntity<String> updateFile(MultipartFile file , String zip_path , String directory_path)
    {
        try
        {
            File file1 = new File( zip_path );
            if(file1.exists())
            {
                file1.delete();
            }
            
            File file2 = new File( directory_path );
            if(file2.exists())
            {
                //delete the directory and all its content
                try
                {
                    Path path = Paths.get( directory_path );
                    DatabaseWriteService.deleteDirectory( path );
                }
                catch(IOException e)
                {
                    LoggingService.logError( e.getMessage() );
                    e.printStackTrace();
                }
            }
            
            Path uploadPath = Paths.get( PATHS.SLAVE_PATH );
            Files.copy( file.getInputStream() , uploadPath.resolve( Objects.requireNonNull( file.getOriginalFilename() ) ) );
        }
        catch(Exception e)
        {
            LoggingService.logError( e.getMessage() );
            throw new RuntimeException( "FAIL!" );
        }
        ZipUtil.unpack( new File( zip_path ) , new File( directory_path ) );
        
        return ResponseEntity.status( HttpStatus.OK ).body( "Uploaded on batchApp" );
    }
    
    private String getDocumentsFromIndex(String database , String collection , String property , String value)
    {
        if(index_storing_service.isCollectionIndexStored( database , collection ))
        {
            Index collection_index = index_storing_service.getCollectionIndex( database , collection );
            if(collection_index.getProperty().equals( property ))
            {
                JSONArray documentsThatHave = collection_index.getDocumentsThatHave( String.valueOf( value ) );
                if(documentsThatHave.length() == 0)
                {
                    return "[]";
                }
                else
                {
                    return documentsThatHave.toString();
                }
            }
        }
        //so we don't have an index for this collection
        requestIndex( database , collection , property );
        
        return index_storing_service.getCollectionIndex( database , collection ).getDocumentsThatHave( String.valueOf( value ) ).toString();
    }
    
    private void requestIndex(String database , String collection , String property)
    {
        String url = "http://MASTER/master/" + database + "/" + collection + "/" + property + "/index";
        //get the index from the master
        String response = rest_template.getForObject( url , String.class );
        index_storing_service.storeCollectionIndex( database , collection , response );
    }
    
    private void requestIndex(String database , String collection)
    {
        requestIndex( database , collection , "id" );
    }
    
    private String getFieldFromIndex(String database , String collection , String property , String value , String field)
    {
        String documents = getDocumentsFromIndex( database , collection , property , value );
        JSONArray documents_json = new JSONArray( documents );
        if(documents_json.length() == 0)
        {
            //raise exception
            throw new IllegalStateException( "No documents found" );
        }
        else
        {
            JSONObject document = documents_json.getJSONObject( 0 );
            return document.getString( field );
        }
    }
    
    private void alert()
    {
        System.out.println(
                "*************************************************************************************************************************" );
        System.out.println( "serving serving" );
        System.out.println(
                "*************************************************************************************************************************" );
    }
}
