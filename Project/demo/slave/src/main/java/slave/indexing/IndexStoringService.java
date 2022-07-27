package slave.indexing;

import org.springframework.stereotype.Service;
import slave.PATHS;
import slave.sharedpackages.indexing.CollectionIndex;
import slave.sharedpackages.indexing.DatabaseIndex;
import slave.sharedpackages.indexing.Index;

import java.util.HashMap;

@Service
public class IndexStoringService
{
    private final HashMap<String, DatabaseIndex> databases_indexes;
    
    public IndexStoringService()
    {
        databases_indexes = new HashMap<>();
    }
    
    //is database index already stored?
    public boolean isDatabaseIndexStored(String database_name)
    {
        return databases_indexes.containsKey( database_name );
    }
    
    public boolean isDatabaseIndexNotStored(String database_name)
    {
        return !isDatabaseIndexStored( database_name );
    }
    
    public DatabaseIndex getDatabaseIndex(String database_name)
    {
        if(!databases_indexes.containsKey( database_name ))
        {
            return null;
        }
        return databases_indexes.get( database_name );
    }
    
    public void deleteDatabaseIndex(String database_name)
    {
        databases_indexes.remove( database_name );
    }
    
    public void storeDatabaseIndex(String database_name , DatabaseIndex database_index)
    {
        databases_indexes.put( database_name , database_index );
    }
    
    public void storeDatabaseIndex(String database_name , String database_index)
    {
        databases_indexes.put( database_name , DatabaseIndex.fromString( PATHS.DATABASES_PATH , database_index ) );
    }
    
    //is collection index already stored?
    public boolean isCollectionIndexStored(String database_name , String collection_name)
    {
        if(!databases_indexes.containsKey( database_name ))
        {
            return false;
        }
        return databases_indexes.get( database_name ).isCollectionExists( collection_name );
    }
    
    public boolean isCollectionIndexNotStored(String database_name , String collection_name)
    {
        return !isCollectionIndexStored( database_name , collection_name );
    }
    
    public Index getCollectionIndex(String database_name , String collection_name)
    {
        if(!databases_indexes.containsKey( database_name ))
        {
            return null;
        }
        return databases_indexes.get( database_name ).getCollectionIndex( collection_name );
    }
    
    public void deleteCollectionIndex(String database_name , String collection_name)
    {
        if(!databases_indexes.containsKey( database_name ))
        {
            return;
        }
        databases_indexes.get( database_name ).removeCollection( collection_name );
    }
    
    public void storeCollectionIndex(String database_name , String collection_name , Index collection_index)
    {
        if(!databases_indexes.containsKey( database_name ))
        {
            databases_indexes.put( database_name , new DatabaseIndex( PATHS.DATABASES_PATH , database_name ) );
        }
        databases_indexes.get( database_name ).addCollection( collection_name , collection_index );
    }
    
    public void storeCollectionIndex(String database_name , String collection_name , String collection_index)
    {
        if(!databases_indexes.containsKey( database_name ))
        {
            databases_indexes.put( database_name , new DatabaseIndex( PATHS.DATABASES_PATH , database_name ) );
        }
        databases_indexes.get( database_name ).addCollection( collection_name , CollectionIndex.fromString( collection_index ) );
    }
}
