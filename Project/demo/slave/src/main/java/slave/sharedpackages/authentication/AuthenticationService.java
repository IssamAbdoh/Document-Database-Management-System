package slave.sharedpackages.authentication;

import javafx.util.Pair;
import org.json.JSONArray;
import org.json.JSONObject;
import slave.sharedpackages.rwoperations.DatabaseReadService;

import java.util.ArrayList;
import java.util.List;

public class AuthenticationService
{
    private static final String database_name = "authorised_db";
    private static final String collection_name = "users";
    
    private AuthenticationService()
    {
    
    }
    
    public static boolean authenticate(String DATABASES_PATH , String username , String password)
    {
        List<Pair<String, String>> pairs = new ArrayList<>();
        pairs.add( new Pair<>( "username" , username ) );
        pairs.add( new Pair<>( "password" , password ) );
        JSONArray documents = DatabaseReadService.getDocuments( DATABASES_PATH , database_name , collection_name , pairs );
        
        assert documents != null;
        return documents.length() != 0;
    }
    
    public static boolean isAdmin(String DATABASES_PATH , String username)
    {
        List<Pair<String, String>> pairs = new ArrayList<>();
        pairs.add( new Pair<>( "username" , username ) );
        pairs.add( new Pair<>( "role" , "admin" ) );
        JSONArray documents = DatabaseReadService.getDocuments( DATABASES_PATH , database_name , collection_name , pairs );
        
        assert documents != null;
        return documents.length() != 0;
    }
    
    public static boolean isNormalUser(String DATABASES_PATH , String username)
    {
        List<Pair<String, String>> pairs = new ArrayList<>();
        pairs.add( new Pair<>( "username" , username ) );
        pairs.add( new Pair<>( "role" , "normal user" ) );
        JSONArray documents = DatabaseReadService.getDocuments( DATABASES_PATH , database_name , collection_name , pairs );
        
        assert documents != null;
        return documents.length() != 0;
    }
    
    public static boolean isAdmin(String DATABASES_PATH , String username , String password)
    {
        List<Pair<String, String>> pairs = new ArrayList<>();
        pairs.add( new Pair<>( "username" , username ) );
        pairs.add( new Pair<>( "password" , password ) );
        pairs.add( new Pair<>( "role" , "admin" ) );
        JSONArray documents = DatabaseReadService.getDocuments( DATABASES_PATH , database_name , collection_name , pairs );
        
        assert documents != null;
        return documents.length() != 0;
    }
    
    public static boolean isNormalUser(String DATABASES_PATH , String username , String password)
    {
        List<Pair<String, String>> pairs = new ArrayList<>();
        pairs.add( new Pair<>( "username" , username ) );
        pairs.add( new Pair<>( "password" , password ) );
        pairs.add( new Pair<>( "role" , "normal user" ) );
        JSONArray documents = DatabaseReadService.getDocuments( DATABASES_PATH , database_name , collection_name , pairs );
        
        assert documents != null;
        return documents.length() != 0;
    }
    
    public static String getRole(String DATABASES_PATH , String username)
    {
        List<Pair<String, String>> pairs = new ArrayList<>();
        pairs.add( new Pair<>( "username" , username ) );
        JSONArray documents = DatabaseReadService.getDocuments( DATABASES_PATH , database_name , collection_name , pairs );
        
        assert documents != null;
        JSONObject document = documents.getJSONObject( 0 );
        return document.getString( "role" );
    }
}
