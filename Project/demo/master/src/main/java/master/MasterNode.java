package master;

import master.sharedpackages.exporting.ExportService;
import master.sharedpackages.exporting.ImportService;
import master.sharedpackages.logging.LoggingService;
import master.sharedpackages.rwoperations.DatabaseWriteService;
import org.json.JSONObject;
import org.springframework.stereotype.Component;


@Component
public class MasterNode
{
    public MasterNode()
    {
        LoggingService.logInfo( "Master Node Created" );
    }
    
    //creating
    
    public void createDatabase(String DATABASES_PATH , String database_name)
    {
        DatabaseWriteService.createDatabase( DATABASES_PATH , database_name );
    }
    
    public void createCollection(String database_name , String collection_name)
    {
        DatabaseWriteService.createCollection( PATHS.DATABASES_PATH , database_name , collection_name );
    }
    
    //inserting
    
    public void insertDocument(String database_name , String collection_name , JSONObject json_object)
    {
        DatabaseWriteService.insertDocument( PATHS.DATABASES_PATH , database_name , collection_name , json_object );
    }
    
    //updating
    
    public void updateDocument(String database_name , String collection_name , long id , JSONObject updated_json_object)
    {
        DatabaseWriteService.updateDocument( PATHS.DATABASES_PATH , database_name , collection_name , id , updated_json_object );
    }
    
    public void updateDatabaseName(String old_database_name , String new_database_name)
    {
        DatabaseWriteService.updateDatabaseName( PATHS.DATABASES_PATH , old_database_name , new_database_name );
    }
    
    public void updateCollectionName(String database_name , String old_collection_name , String new_collection_name)
    {
        DatabaseWriteService.updateCollectionName( PATHS.DATABASES_PATH , database_name , old_collection_name , new_collection_name );
    }
    
    //deleting
    
    public void deleteDatabase(String database_name)
    {
        DatabaseWriteService.deleteDatabase( PATHS.DATABASES_PATH , database_name );
    }
    
    public void deleteCollection(String database_name , String collection_name)
    {
        DatabaseWriteService.deleteCollection( PATHS.DATABASES_PATH , database_name , collection_name );
    }
    
    public void deleteDocument(String database_name , String collection_name , long document_id)
    {
        DatabaseWriteService.deleteDocument( PATHS.DATABASES_PATH , database_name , collection_name , document_id );
    }
    
    public void deleteProperty(String database_name , String collection_name , long document_id , String property_name)
    {
        DatabaseWriteService.deleteProperty( PATHS.DATABASES_PATH , database_name , collection_name , document_id , property_name );
    }
    
    //exporting
    
    public void exportDatabase(String database_name , String schema_name)
    {
        ExportService.exportSchema( PATHS.EXPORT_DIRECTORY_PATH , PATHS.DATABASES_PATH , database_name , schema_name );
    }
    
    public void exportLocally(String database_name , String schema_name , String path)
    {
        ExportService.exportSchemaTo( PATHS.DATABASES_PATH , database_name , schema_name , path );
    }
    
    //importing
    
    public void importDatabase(String schema_name)
    {
        ImportService.importSchema( PATHS.EXPORT_DIRECTORY_PATH , PATHS.DATABASES_PATH , schema_name );
    }
    
    public void importLocally(String path)
    {
        ImportService.importSchemaFromAPath( PATHS.DATABASES_PATH , path );
    }
}
