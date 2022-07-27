package master.sharedpackages.indexing;

import master.sharedpackages.logging.LoggingService;
import master.sharedpackages.rwoperations.DatabaseReadService;
import org.json.JSONArray;
import org.json.JSONObject;

import java.util.HashMap;
import java.util.Objects;

public class CollectionIndex implements Index
{
    private String DATABASES_PATH;
    private String property;
    private String database;
    private String collection;
    
    private HashMap<String, JSONArray> index;
    
    public CollectionIndex(String DATABASES_PATH , String database , String collection , String property)
    {
        this.DATABASES_PATH = DATABASES_PATH;
        this.property = property;
        this.database = database;
        this.collection = collection;
        populate();
    }
    
    public CollectionIndex(String DATABASES_PATH , String database , String collection)
    {
        this( DATABASES_PATH , database , collection , "id" );
    }
    
    private void setProperty(String property)
    {
        this.property = property;
    }
    
    public void setPropertyAndPopulate(String property)
    {
        setProperty( property );
        this.populate();
    }
    
    private void populate()
    {
        this.index = new HashMap<>();
        
        JSONArray all_documents = DatabaseReadService.getCollection( this.DATABASES_PATH , this.database , this.collection );
        if(all_documents == null)
        {
            LoggingService.logError( "Indexing failed path: " + this.database + "." + this.collection + " is not found" );
            throw new NullPointerException( "Indexing failed path: " + this.database + "." + this.collection + " is " + "not found" );
        }
        for(int i = 0 ; i < Objects.requireNonNull( all_documents ).length() ; i++)
        {
            JSONObject document = all_documents.getJSONObject( i );
            if(document.has( this.property ))
            {
                String key = document.get( this.property ).toString();
                if(!this.index.containsKey( key ))
                {
                    this.index.put( key , new JSONArray() );
                }
                this.index.get( key ).put( document );
            }
            else
            {
                String key = "";
                if(!this.index.containsKey( key ))
                {
                    this.index.put( key , new JSONArray() );
                }
                this.index.get( key ).put( document );
            }
        }
        
    }
    
    public void updateCollectionIndex()
    {
        this.populate();
    }
    
    public JSONArray getDocumentsThatHave(String value)
    {
        if(this.index.containsKey( value ))
        {
            return this.index.get( value );
        }
        return new JSONArray();
    }
    
    public void setCollectionAndPopulate(String collection)
    {
        setCollection( collection );
        this.populate();
    }
    
    private void setCollection(String collection)
    {
        this.collection = collection;
    }
    
    public String getProperty()
    {
        return property;
    }
    
    public String getDatabase()
    {
        return database;
    }
    
    public String getCollection()
    {
        return collection;
    }
    
    //generated :
    public Index getIndex()
    {
        return (Index) this.index;
    }
    
    //generated :
    private void setIndex(HashMap<String, JSONArray> index)
    {
        this.index = index;
    }
    
    public CollectionIndex()
    {
    }
    
    @Override
    public String toString()
    {
        final StringBuffer sb = new StringBuffer( "{" );
        sb.append( "property:'" ).append( property ).append( '\'' );
        sb.append( ", database:'" ).append( database ).append( '\'' );
        sb.append( ", collection:'" ).append( collection ).append( '\'' );
        sb.append( ", index:" ).append( new JSONObject( index ) );
        sb.append( '}' );
        return sb.toString();
    }
    
    public static CollectionIndex fromString(String s)
    {
        JSONObject json = new JSONObject( s );
        CollectionIndex index = new CollectionIndex();
        index.setProperty( json.getString( "property" ) );
        index.setDatabase( json.getString( "database" ) );
        index.setCollection( json.getString( "collection" ) );
        JSONObject index1 = json.getJSONObject( "index" );
        HashMap<String, JSONArray> index2 = new HashMap<>();
        for(String key : index1.keySet())
        {
            index2.put( key , index1.getJSONArray( key ) );
        }
        index.setIndex( index2 );
        return index;
    }
    
    private void setDatabase(String database)
    {
        this.database = database;
    }
}
