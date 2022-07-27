package master.sharedpackages.rwoperations;

import javafx.util.Pair;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

public class DatabaseReadService
{
    private DatabaseReadService()
    {
    }
    
    public static JSONObject getDocument(String DATABASES_PATH , String database_name , String collection_name , long id)
    {
        database_name = database_name.toLowerCase();
        collection_name = collection_name.toLowerCase();
        
        
        //check if the file exists
        if(isCollectionExists( DATABASES_PATH , database_name , collection_name ))
        {
            try
            {
                Path path = Paths.get( DATABASES_PATH + "/" + database_name + "/" + collection_name + ".json" );
                String content = new String( Files.readAllBytes( path ) );
                JSONArray json_array = new JSONArray( content );
                for(int i = 0 ; i < json_array.length() ; i++)
                {
                    JSONObject json_object = json_array.getJSONObject( i );
                    if(json_object.getLong( "id" ) == id)
                    {
                        return json_object;
                    }
                }
                return new JSONObject();
            }
            catch(IOException e)
            {
                e.printStackTrace();
            }
        }
        throw new IllegalStateException( "Collection " + database_name + "." + collection_name + " is not found" );
    }
    
    //return field in the document with the given id
    public static String getField(String DATABASES_PATH , String database_name , String collection_name , long id , String field)
    {
        database_name = database_name.toLowerCase();
        collection_name = collection_name.toLowerCase();
        
        
        //check if the file exists
        if(isCollectionExists( DATABASES_PATH , database_name , collection_name ))
        {
            try
            {
                Path path = Paths.get( DATABASES_PATH + "/" + database_name + "/" + collection_name + ".json" );
                String content = new String( Files.readAllBytes( path ) );
                JSONArray json_array = new JSONArray( content );
                for(int i = 0 ; i < json_array.length() ; i++)
                {
                    JSONObject json_object = json_array.getJSONObject( i );
                    if(json_object.getLong( "id" ) == id)
                    {
                        if(json_object.has( field ))
                        {
                            return json_object.getString( field );
                        }
                        else
                        {
                            return "";
                        }
                    }
                }
                throw new IllegalStateException( "Document with id " + id + " is not found" );
            }
            
            catch(IOException e)
            {
                e.printStackTrace();
            }
        }
        throw new IllegalStateException( "Collection " + database_name + "." + collection_name + " is not found" );
    }
    
    //return all the documents in the collection
    public static JSONArray getCollection(String DATABASES_PATH , String database_name , String collection_name)
    {
        database_name = database_name.toLowerCase();
        collection_name = collection_name.toLowerCase();
        
        
        Path path = Paths.get( DATABASES_PATH + "/" + database_name + "/" + collection_name + ".json" );
        
        //check if the file exists
        if(isCollectionExists( DATABASES_PATH , database_name , collection_name ))
        {
            try
            {
                String content = new String( Files.readAllBytes( path ) );
                return new JSONArray( content );
            }
            catch(IOException e)
            {
                e.printStackTrace();
            }
        }
        throw new IllegalStateException( "Collection " + database_name + "." + collection_name + " is not found" );
    }
    
    public static boolean isDocumentExists(String DATABASES_PATH , String database_name , String collection_name , String field , String value)
    {
        database_name = database_name.toLowerCase();
        collection_name = collection_name.toLowerCase();
        
        String s = database_name + "/" + collection_name + ".json";
        
        Path path = Paths.get( DATABASES_PATH + "/" + s );
        
        //check if the file exists
        if(isCollectionExists( DATABASES_PATH , database_name , collection_name ))
        {
            try
            {
                String content = new String( Files.readAllBytes( path ) );
                JSONArray json_array = new JSONArray( content );
                for(int i = 0 ; i < json_array.length() ; i++)
                {
                    JSONObject json_object = json_array.getJSONObject( i );
                    if(json_object.has( field ) && json_object.getString( field ).equals( value ))
                    {
                        return true;
                    }
                }
            }
            catch(IOException e)
            {
                e.printStackTrace();
            }
        }
        return false;
    }
    
    public static boolean isDocumentExists(String DATABASES_PATH , String database_name , String collection_name , long id)
    {
        database_name = database_name.toLowerCase();
        collection_name = collection_name.toLowerCase();
        
        String s = database_name + "/" + collection_name + ".json";
        
        Path path = Paths.get( DATABASES_PATH + "/" + s );
        
        //check if the file exists
        if(isCollectionExists( DATABASES_PATH , database_name , collection_name ))
        {
            try
            {
                String content = new String( Files.readAllBytes( path ) );
                JSONArray json_array = new JSONArray( content );
                for(int i = 0 ; i < json_array.length() ; i++)
                {
                    JSONObject json_object = json_array.getJSONObject( i );
                    if(json_object.getLong( "id" ) == id)
                    {
                        return true;
                    }
                }
            }
            catch(IOException e)
            {
                e.printStackTrace();
            }
        }
        return false;
    }
    
    public static ArrayList<JSONArray> getDatabase(String DATABASES_PATH , String database_name)
    {
        database_name = database_name.toLowerCase();
        
        String s = database_name + "/";
        
        Path path = Paths.get( DATABASES_PATH + "/" + s );
        
        //check if the file exists
        if(isDatabaseExists( DATABASES_PATH , database_name ))
        {
            try
            {
                //for each file in the directory call getAllDocuments(database_name , collection_name)
                ArrayList<JSONArray> json_arrays = new ArrayList<>();
                String finalDatabase_name = database_name;
                Files.walk( path ).forEach( filePath ->
                {
                    if(Files.isRegularFile( filePath ))
                    {
                        //get file name
                        String file_name = filePath.getFileName().toString();
                        
                        json_arrays.add( getCollection( DATABASES_PATH , finalDatabase_name , file_name.substring( 0 , file_name.length() - 5 ) ) );
                    }
                } );
                
                return json_arrays;
            }
            catch(IOException e)
            {
                e.printStackTrace();
            }
        }
        return null;
    }
    
    public static boolean isCollectionExists(String DATABASES_PATH , String database_name , String collection_name)
    {
        database_name = database_name.toLowerCase();
        collection_name = collection_name.toLowerCase();
        
        String s = database_name + "/" + collection_name + ".json";
        
        Path path = Paths.get( DATABASES_PATH + "/" + s );
        
        //check if the file exists
        return Files.exists( path );
    }
    
    public static boolean isCollectionNotExists(String DATABASES_PATH , String database_name , String collection_name)
    {
        return !isCollectionExists( DATABASES_PATH , database_name , collection_name );
    }
    
    public static boolean isDatabaseExists(String DATABASES_PATH , String database_name)
    {
        database_name = database_name.toLowerCase();
        
        String s = database_name + "/";
        
        Path path = Paths.get( DATABASES_PATH + "/" + s );
        
        //check if the file exists
        return Files.exists( path );
    }
    
    public static boolean isDatabaseNotExists(String DATABASES_PATH , String database_name)
    {
        return !isDatabaseExists( DATABASES_PATH , database_name );
    }
    
    public static ArrayList<String> getAllCollectionNames(String DATABASES_PATH , String database_name)
    {
        database_name = database_name.toLowerCase();
        
        
        Path path = Paths.get( DATABASES_PATH + "/" + database_name + "/" );
        
        //check if the file exists
        if(isDatabaseExists( DATABASES_PATH , database_name ))
        {
            try
            {
                ArrayList<String> collection_names = new ArrayList<>();
                Files.walk( path ).forEach( filePath ->
                {
                    if(Files.isRegularFile( filePath ))
                    {
                        //get file name
                        String file_name = filePath.getFileName().toString();
                        collection_names.add( file_name.substring( 0 , file_name.length() - 5 ) );
                    }
                } );
                return collection_names;
            }
            catch(IOException e)
            {
                e.printStackTrace();
            }
        }
        
        return null;
        
    }
    
    public static JSONArray getDocuments(String DATABASES_PATH , String database_name , String collection_name , String field , String value)
    {
        database_name = database_name.toLowerCase();
        collection_name = collection_name.toLowerCase();
        
        String s = database_name + "/" + collection_name + ".json";
        
        Path path = Paths.get( DATABASES_PATH + "/" + s );
        
        //check if the file exists
        if(isCollectionExists( DATABASES_PATH , database_name , collection_name ))
        {
            try
            {
                String content = new String( Files.readAllBytes( path ) );
                JSONArray json_array = new JSONArray( content );
                JSONArray filtered_json_array = new JSONArray();
                for(int i = 0 ; i < json_array.length() ; i++)
                {
                    JSONObject json_object = json_array.getJSONObject( i );
                    if(json_object.has( field ) && json_object.getString( field ).equals( value ))
                    {
                        filtered_json_array.put( json_object );
                    }
                }
                return filtered_json_array;
            }
            catch(IOException e)
            {
                e.printStackTrace();
            }
        }
        throw new IllegalArgumentException( "Collection does not exist" );
    }
    
    public static JSONArray getDocuments(String DATABASES_PATH , String database_name , String collection_name ,
                                         List<Pair<String, String>> properties)
    {
        database_name = database_name.toLowerCase();
        collection_name = collection_name.toLowerCase();
        
        String s = database_name + "/" + collection_name + ".json";
        
        Path path = Paths.get( DATABASES_PATH + "/" + s );
        
        //check if the file exists
        if(isCollectionExists( DATABASES_PATH , database_name , collection_name ))
        {
            try
            {
                String content = new String( Files.readAllBytes( path ) );
                JSONArray json_array = new JSONArray( content );
                JSONArray filtered_json_array = new JSONArray();
                for(int i = 0 ; i < json_array.length() ; i++)
                {
                    JSONObject json_object = json_array.getJSONObject( i );
                    boolean is_valid = true;
                    for(Pair<String, String> property : properties)
                    {
                        if(!json_object.has( property.getKey() ) || !json_object.getString( property.getKey() ).equals( property.getValue() ))
                        {
                            is_valid = false;
                            break;
                        }
                    }
                    if(is_valid)
                    {
                        filtered_json_array.put( json_object );
                    }
                }
                return filtered_json_array;
            }
            catch(IOException e)
            {
                e.printStackTrace();
            }
        }
        return null;
    }
}
