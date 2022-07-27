package master.sharedpackages.indexing;

import master.sharedpackages.rwoperations.DatabaseReadService;
import org.json.JSONArray;
import org.json.JSONObject;

import java.util.HashMap;

public class DatabaseIndex
{
    private String database_name;
    private final String DATABASES_PATH;
    private HashMap<String, Index> collection_indexes;
    
    private DatabaseIndex(String DATABASES_PATH)
    {
        this.DATABASES_PATH = DATABASES_PATH;
        this.collection_indexes = new HashMap<>();
    }
    
    public DatabaseIndex(String DATABASES_PATH , String database_name)
    {
        this.database_name = database_name;
        this.DATABASES_PATH = DATABASES_PATH;
        this.collection_indexes = new HashMap<>();
    }
    
    public void updateCollectionIndex(String collection_name)
    {
        if(collection_indexes.containsKey( collection_name ))
        {
            this.collection_indexes.get( collection_name ).updateCollectionIndex();
        }
        else
        {
            if(DatabaseReadService.isCollectionExists( this.DATABASES_PATH , this.database_name , collection_name ))
            {
                this.collection_indexes.put( collection_name , new CollectionIndex( this.DATABASES_PATH , this.database_name , collection_name ) );
            }
        }
    }
    
    public void updateCollectionIndex(String collection_name , String property_name)
    {
        if(collection_indexes.containsKey( collection_name ))
        {
            this.collection_indexes.get( collection_name ).setPropertyAndPopulate( property_name );
        }
        else
        {
            if(DatabaseReadService.isCollectionExists( this.DATABASES_PATH , this.database_name , collection_name ))
            {
                this.collection_indexes.put( collection_name , new CollectionIndex( this.database_name , collection_name , property_name ) );
            }
        }
    }
    
    public JSONObject getDocument(String DATABASES_PATH , String collection_name , long id)
    {
        if(collection_indexes.containsKey( collection_name ))
        {
            if(this.collection_indexes.get( collection_name ).getProperty().equals( "id" ))//O(1)
            {
                JSONArray documentsThatHave = this.collection_indexes.get( collection_name ).getDocumentsThatHave( String.valueOf( id ) );
                if(documentsThatHave != null)
                {
                    return documentsThatHave.getJSONObject( 0 );
                }
            }
            else//O(n)
            {
                return DatabaseReadService.getDocument( DATABASES_PATH , this.database_name , collection_name , id );
            }
        }
        return null;
    }
    
    public JSONArray getDocuments(String DATABASES_PATH , String collection_name , String property , String value)
    {
        if(collection_indexes.containsKey( collection_name ))
        {
            if(this.collection_indexes.get( collection_name ).getProperty().equals( property ))//O(1)
            {
                return this.collection_indexes.get( collection_name ).getDocumentsThatHave( value );
            }
            else//O(n)
            {
                return DatabaseReadService.getDocuments( DATABASES_PATH , this.database_name , collection_name , property , value );
            }
        }
        return null;
    }
    
    public Index getCollectionIndex(String collection_name)
    {
        if(collection_indexes.containsKey( collection_name ))
        {
            return this.collection_indexes.get( collection_name );
        }
        return null;
    }
    
    public String getDatabaseName()
    {
        return this.database_name;
    }
    
    public int getNumberOfCollections()
    {
        return this.collection_indexes.size();
    }
    
    @Override
    public String toString()
    {
        final StringBuffer sb = new StringBuffer( "{" );
        sb.append( "database_name:'" ).append( database_name ).append( "'" );
        sb.append( ", collection_indexes:" )//append the collection indexes but remove the backslashes
                .append( collection_indexes.toString().replaceAll( "\\\\" , "" ).replaceAll( "=" , ":" ) );
        sb.append( "}" );
        return sb.toString();
    }
    
    //from string to DatabaseIndex
    public static DatabaseIndex fromString(String DATABASES_PATH , String string)
    {
        JSONObject json = new JSONObject( string );
        String database_name = json.getString( "database_name" );
        JSONObject collection_indexes = json.getJSONObject( "collection_indexes" );
        DatabaseIndex database_index = new DatabaseIndex( DATABASES_PATH );
        database_index.database_name = database_name;
        database_index.collection_indexes = new HashMap<>();
        for(String collection_name : collection_indexes.keySet())
        {
            database_index.collection_indexes.put( collection_name ,
                    CollectionIndex.fromString( collection_indexes.getJSONObject( collection_name ).toString() ) );
        }
        
        return database_index;
    }
    
    public void addCollection(String collection_name)
    {
        this.collection_indexes.put( collection_name , new CollectionIndex( this.DATABASES_PATH , this.database_name , collection_name ) );
    }
    
    public void addCollection(String DATABASES_PATH , String collection_name , String property)
    {
        this.collection_indexes.put( collection_name , new CollectionIndex( DATABASES_PATH , this.database_name , collection_name , property ) );
    }
    
    public void addCollection(String collection_name , Index collection_index)
    {
        this.collection_indexes.put( collection_name , collection_index );
    }
    
    public void removeCollection(String collection_name)
    {
        this.collection_indexes.remove( collection_name );
    }
    
    public boolean isCollectionExists(String collection_name)
    {
        return this.collection_indexes.containsKey( collection_name );
    }
    
    public boolean isCollectionNotExists(String collection_name)
    {
        return !isCollectionExists( collection_name );
    }
    
}
