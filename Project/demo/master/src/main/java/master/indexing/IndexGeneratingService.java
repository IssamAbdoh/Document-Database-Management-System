package master.indexing;

import master.PATHS;
import master.sharedpackages.indexing.DatabaseIndex;
import master.sharedpackages.indexing.Index;
import org.springframework.stereotype.Service;

import java.util.HashMap;

@Service
public class IndexGeneratingService
{
    private final HashMap<String, DatabaseIndex> databases_indexes;
    
    public IndexGeneratingService()
    {
        databases_indexes = new HashMap<>();
    }
    
    public void deleteDatabaseIndex(String database_name)
    {
        databases_indexes.remove( database_name );
    }
    
    public Index getCollectionIndex(String database_name , String collection_name , String property)
    {
        if(!databases_indexes.containsKey( database_name ))
        {
            databases_indexes.put( database_name , new DatabaseIndex( PATHS.DATABASES_PATH , database_name ) );
        }
        if(databases_indexes.get( database_name ).isCollectionNotExists( collection_name ))
        {
            databases_indexes.get( database_name ).addCollection( PATHS.DATABASES_PATH , collection_name , property );
        }
        else if(!databases_indexes.get( database_name ).getCollectionIndex( collection_name ).getProperty().equals( property ))
        {
            databases_indexes.get( database_name ).getCollectionIndex( collection_name ).setPropertyAndPopulate( property );
        }
        
        DatabaseIndex database_index = databases_indexes.get( database_name );
        return database_index.getCollectionIndex( collection_name );
    }
    
    public void deleteCollectionIndex(String database_name , String collection_name)
    {
        if(!databases_indexes.containsKey( database_name ))
        {
            return;
        }
        DatabaseIndex database_index = databases_indexes.get( database_name );
        database_index.removeCollection( collection_name );
    }
    
    
}
