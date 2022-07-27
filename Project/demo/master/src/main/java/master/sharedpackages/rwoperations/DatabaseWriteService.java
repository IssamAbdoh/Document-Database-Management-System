package master.sharedpackages.rwoperations;

import master.PATHS;
import master.sharedpackages.logging.LoggingService;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.File;
import java.io.IOException;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.concurrent.locks.ReentrantLock;


public class DatabaseWriteService
{
    static Path sequencing_file_path = Paths.get( PATHS.SEQUENCING_FILE_PATH );
    private static final ReentrantLock reentrant_lock_1 = new ReentrantLock( true );
    private static final ReentrantLock reentrant_lock_2 = new ReentrantLock( true );
    private static final ReentrantLock reentrant_lock_3 = new ReentrantLock( true );
    
    
    private DatabaseWriteService()
    {
    }
    
    private static long getNextId(String database_name , String collection_name)
    {
        database_name = database_name.toLowerCase();
        collection_name = collection_name.toLowerCase();
        
        JSONObject json_object = getSequencingFileContent();
        
        String s = database_name + "/" + collection_name;
        
        //get the value of database_name/collection_name
        long value;
        //is s in json_object?
        if(json_object.has( s ))
        {
            //get the value of s
            value = json_object.getLong( s );
        }
        else
        {
            value = 0;
        }
        
        return value + 1;
    }
    
    private static void updateSequencing(String database_name , String collection_name , long new_id)
    {
        database_name = database_name.toLowerCase();
        collection_name = collection_name.toLowerCase();
        
        String s = database_name + "/" + collection_name;
        
        JSONObject json_object = getSequencingFileContent();
        
        json_object.put( s , new_id );
        
        //write the json object to the file
        try
        {
            Files.write( sequencing_file_path , json_object.toString().getBytes() );
        }
        catch(IOException e)
        {
            e.printStackTrace();
        }
        
        LoggingService.logInfo( "Sequencing updated !" );
    }
    
    private static void renameDatabaseInSequencingFile(String database_name , String new_database_name)
    {
        database_name = database_name.toLowerCase();
        new_database_name = new_database_name.toLowerCase();
        
        JSONObject json_object = getSequencingFileContent();
        
        String s = database_name;
        String new_s = new_database_name;
        
        //loop over all the keys of the json object
        for(String key : json_object.keySet())
        {
            //is key contains database_name?
            if(key.split( "/" )[0].equals( s ))
            {
                //replace database_name by new_database_name
                String new_key = key.replace( s , new_s );
                //put the new key and the value of the old key
                json_object.put( new_key , json_object.get( key ) );
                //remove the old key
                json_object.remove( key );
            }
        }
        
        //write the json object to the file
        try
        {
            Files.write( sequencing_file_path , json_object.toString().getBytes() );
        }
        catch(IOException e)
        {
            e.printStackTrace();
        }
        
        LoggingService.logInfo( "Sequencing updated !" );
    }
    
    private static JSONObject getSequencingFileContent()
    {
        //check if the file exists
        if(!sequencing_file_path.toFile().exists())
        {
            //create the file
            try
            {
                sequencing_file_path.toFile().createNewFile();
            }
            catch(IOException e)
            {
                e.printStackTrace();
            }
            LoggingService.logInfo( "File sequencing.json created" );
        }
        else
        {
            LoggingService.logWarn( "File sequencing.json already exists" );
        }
        
        //put the content of the file in a string
        String content = null;
        try
        {
            content = new String( Files.readAllBytes( sequencing_file_path ) );
        }
        catch(IOException e)
        {
            e.printStackTrace();
        }
        
        //parse the content of the file to json object
        JSONObject json_object = new JSONObject( content );
        return json_object;
    }
    
    //renameCollectionInSequencingFile
    private static void renameCollectionInSequencingFile(String database_name , String old_collection_name , String new_collection_name)
    {
        database_name = database_name.toLowerCase();
        old_collection_name = old_collection_name.toLowerCase();
        new_collection_name = new_collection_name.toLowerCase();
        
        JSONObject json_object = getSequencingFileContent();
        
        String s = database_name + "/" + old_collection_name;
        String new_s = database_name + "/" + new_collection_name;
        
        //loop over all the keys of the json object
        for(String key : json_object.keySet())
        {
            //is key contains database_name?
            if(key.equals( s ))
            {
                //replace database_name by new_database_name
                String new_key = key.replace( s , new_s );
                //put the new key and the value of the old key
                json_object.put( new_key , json_object.get( key ) );
                //remove the old key
                json_object.remove( key );
            }
        }
        
        //write the json object to the file
        
        try
        {
            Files.write( sequencing_file_path , json_object.toString().getBytes() );
        }
        catch(IOException e)
        {
            e.printStackTrace();
        }
        
        LoggingService.logInfo( "Sequencing updated !" );
    }
    
    //deleteDatabaseInSequencingFile
    private static void deleteDatabaseInSequencingFile(String database_name)
    {
        database_name = database_name.toLowerCase();
        
        JSONObject json_object = getSequencingFileContent();
        
        String s = database_name;
        
        //loop over all the keys of the json object
        for(String key : json_object.keySet())
        {
            //is key contains database_name?
            if(key.split( "/" )[0].equals( s ))
            {
                //remove the old key
                json_object.remove( key );
            }
        }
        
        //write the json object to the file
        try
        {
            Files.write( sequencing_file_path , json_object.toString().getBytes() );
        }
        catch(IOException e)
        {
            e.printStackTrace();
        }
        
        LoggingService.logInfo( "Sequencing updated !" );
    }
    
    //deleteCollectionInSequencingFile
    private static void deleteCollectionInSequencingFile(String database_name , String collection_name)
    {
        database_name = database_name.toLowerCase();
        collection_name = collection_name.toLowerCase();
        
        JSONObject json_object = getSequencingFileContent();
        
        String s = database_name + "/" + collection_name;
        
        //loop over all the keys of the json object
        for(String key : json_object.keySet())
        {
            //is key contains database_name?
            if(key.equals( s ))
            {
                //remove the old key
                json_object.remove( key );
            }
        }
        
        //write the json object to the file
        try
        {
            Files.write( sequencing_file_path , json_object.toString().getBytes() );
        }
        catch(IOException e)
        {
            e.printStackTrace();
        }
        
        LoggingService.logInfo( "Sequencing updated !" );
    }
    
    private static boolean isValidName(String name)
    {
        //name should not be empty or null and should not contain . / \
        return name != null && !name.isEmpty() && !name.contains( "." ) && !name.contains( "/" ) && !name.contains( "\\" );
    }
    
    private static boolean isNotValidName(String name)
    {
        return !isValidName( name );
    }
    
    //creating
    
    public static void createDatabase(String DATABASES_PATH , String database_name)
    {
        reentrant_lock_3.lock();
        
        try
        {
            //check if data directory exists
            if(!new File( DATABASES_PATH ).exists())
            {
                //create the directory
                new File( DATABASES_PATH ).mkdir();
                LoggingService.logInfo( "Directory " + DATABASES_PATH + " created" );
            }
            else
            {
                LoggingService.logWarn( "Directory " + DATABASES_PATH + " already exists" );
            }
            
            database_name = database_name.toLowerCase();
            if(isNotValidName( database_name ))
            {
                LoggingService.logError( "Database name is not valid" );
                throw new IllegalArgumentException( "Database name is not valid" );
            }
            
            Path path = Paths.get( DATABASES_PATH + "/" + database_name );
            
            //check if the directory exists
            if(DatabaseReadService.isDatabaseNotExists( DATABASES_PATH , database_name ))
            {
                //create the directory
                try
                {
                    Files.createDirectory( path );
                }
                catch(IOException e)
                {
                    e.printStackTrace();
                }
                LoggingService.logInfo( "Database " + database_name + " created" );
            }
            else
            {
                LoggingService.logError( "Database " + database_name + " already exists" );
                throw new IllegalArgumentException( "Database " + database_name + " already exists" );
            }
        }
        finally
        {
            reentrant_lock_3.unlock();
        }
    }
    
    public static void createCollection(String DATABASES_PATH , String database_name , String collection_name)
    {
        reentrant_lock_2.lock();
        
        try
        {
            
            database_name = database_name.toLowerCase();
            
            collection_name = collection_name.toLowerCase();
            
            if(isNotValidName( database_name ))
            {
                LoggingService.logError( "Database name is not valid" );
                throw new IllegalArgumentException( "Database name is not valid" );
            }
            
            if(isNotValidName( collection_name ))
            {
                LoggingService.logError( "Collection name is not valid" );
                throw new IllegalArgumentException( "Collection name is not valid" );
            }
            
            //check if the database exists
            if(DatabaseReadService.isDatabaseNotExists( DATABASES_PATH , database_name ))
            {
                createDatabase( DATABASES_PATH , database_name );
            }
            
            Path path = Paths.get( DATABASES_PATH + "/" + database_name + "/" + collection_name + ".json" );
            
            //check if the file exists
            if(!path.toFile().exists())
            {
                //create the file
                try
                {
                    path.toFile().createNewFile();
                    Files.write( path , "[]".getBytes() );
                }
                catch(IOException e)
                {
                    e.printStackTrace();
                }
                LoggingService.logInfo( "File " + collection_name + ".json created" );
            }
            else
            {
                throw new IllegalArgumentException( "Collection " + collection_name + " already exists" );
            }
        }
        finally
        {
            reentrant_lock_2.unlock();
        }
    }
    
    //inserting
    
    public static void insertDocument(String DATABASES_PATH , String database_name , String collection_name , JSONObject json_object)
    {
        reentrant_lock_1.lock();
        
        try
        {
            
            database_name = database_name.toLowerCase();
            collection_name = collection_name.toLowerCase();
            
            // path = path + /collection_name.json
            Path path = Paths.get( DATABASES_PATH + "/" + database_name + "/" + collection_name + ".json" );
            
            //create json array
            JSONArray json_array = new JSONArray();
            
            //check if the file exists
            if(!path.toFile().exists())
            {
                //create the file
                try
                {
                    createCollection( DATABASES_PATH , database_name , collection_name );
                    //write [] to the file
                    Files.write( path , json_array.toString().getBytes() );
                }
                catch(IOException e)
                {
                    e.printStackTrace();
                }
                LoggingService.logInfo( "File " + collection_name + ".json created" );
            }
            else
            {
                LoggingService.logInfo( "File " + collection_name + ".json already exists" );
                try
                {
                    json_array = new JSONArray( new String( Files.readAllBytes( path ) ) );
                }
                catch(IOException e)
                {
                    e.printStackTrace();
                }
            }
            
            long id;
            if(!json_object.has( "id" ))
            {
                while(true)
                {
                    id = getNextId( database_name , collection_name );
                    if(!DatabaseReadService.isDocumentExists( DATABASES_PATH , database_name , collection_name , id ))
                    {
                        break;
                    }
                    else
                    {
                        updateSequencing( database_name , collection_name , id );
                    }
                }
                json_object.put( "id" , id );
            }
            else
            {
                id = json_object.getLong( "id" );
                //check if the id is already in the file
                if(DatabaseReadService.isDocumentExists( DATABASES_PATH , database_name , collection_name , id ))
                {
                    LoggingService.logError( "Document with id " + id + " already exists" );
                    throw new IllegalStateException( "Document with id " + id + " already exists" );
                }
            }
            
            json_array.put( json_object );
            
            //write the json array to the file
            try
            {
                Files.write( path , json_array.toString().getBytes() );
            }
            catch(IOException e)
            {
                e.printStackTrace();
            }
            
            LoggingService.logInfo( "Document inserted !" );
            updateSequencing( database_name , collection_name , id );
        }
        finally
        {
            reentrant_lock_1.unlock();
        }
    }
    
    //updating
    
    public static void updateDocument(String DATABASES_PATH , String database_name , String collection_name , long id ,
                                      JSONObject updated_json_object)
    {
        reentrant_lock_1.lock();
        
        try
        {
            database_name = database_name.toLowerCase();
            collection_name = collection_name.toLowerCase();
            
            Path path = Paths.get( DATABASES_PATH + "/" + database_name + "/" + collection_name + ".json" );
            
            //check if the file exists
            if(DatabaseReadService.isCollectionNotExists( DATABASES_PATH , database_name , collection_name ))
            {
                LoggingService.logError( "File " + database_name + "/" + collection_name + ".json doesn't exist" );
                throw new IllegalArgumentException( "File " + database_name + "/" + collection_name + ".json doesn't exist" );
            }
            
            //put the content of the file in a string
            String content = null;
            try
            {
                content = new String( Files.readAllBytes( path ) );
            }
            catch(IOException e)
            {
                e.printStackTrace();
            }
            
            //parse the content of the file to json object
            JSONArray json_array = new JSONArray( content );
            
            //find the document to update
            for(int i = 0 ; i < json_array.length() ; i++)
            {
                JSONObject json_object = json_array.getJSONObject( i );
                
                if(json_object.getLong( "id" ) == id)
                {
                    json_array.remove( i );
                    json_array.put( i , updated_json_object );
                    break;
                }
            }
            
            //write the json array to the file
            try
            {
                Files.write( path , json_array.toString().getBytes() );
            }
            catch(IOException e)
            {
                e.printStackTrace();
            }
            
            LoggingService.logInfo( "Document updated !" );
        }
        finally
        {
            reentrant_lock_1.unlock();
        }
    }
    
    public static void updateDatabaseName(String DATABASES_PATH , String old_database_name , String new_database_name)
    {
        reentrant_lock_1.lock();
        
        try
        {
            
            old_database_name = old_database_name.toLowerCase();
            new_database_name = new_database_name.toLowerCase();
            
            if(isNotValidName( new_database_name ))
            {
                LoggingService.logError( "Database name is not valid" );
                throw new IllegalArgumentException( "Database name is not valid" );
            }
            
            
            Path path1 = Paths.get( DATABASES_PATH + "/" + old_database_name );
            
            //check if the directory exists
            if(!path1.toFile().exists())
            {
                LoggingService.logError( "Directory " + old_database_name + " doesn't exist" );
                return;
            }
            
            //check if the new directory already exists
            Path path2 = Paths.get( DATABASES_PATH + "/" + new_database_name );
            
            if(path2.toFile().exists())
            {
                LoggingService.logError( "Directory " + new_database_name + " already exists" );
                throw new IllegalArgumentException( "Database " + new_database_name + " already exists" );
            }
            
            //rename the directory
            path1.toFile().renameTo( new File( DATABASES_PATH + "/" + new_database_name ) );
            renameDatabaseInSequencingFile( old_database_name , new_database_name );
        }
        finally
        {
            reentrant_lock_1.unlock();
        }
    }
    
    public static void updateCollectionName(String DATABASES_PATH , String database_name , String old_collection_name , String new_collection_name)
    {
        reentrant_lock_1.lock();
        try
        {
            
            database_name = database_name.toLowerCase();
            old_collection_name = old_collection_name.toLowerCase();
            new_collection_name = new_collection_name.toLowerCase();
            
            if(isNotValidName( new_collection_name ))
            {
                LoggingService.logError( "Collection name is not valid" );
                throw new IllegalArgumentException( "Collection name is not valid" );
            }
            
            Path path1 = Paths.get( DATABASES_PATH + "/" + database_name + "/" + old_collection_name + ".json" );
            
            //check if the file exists
            if(!path1.toFile().exists())
            {
                LoggingService.logError( "File " + old_collection_name + ".json doesn't exist" );
                throw new IllegalArgumentException( "File " + old_collection_name + ".json doesn't exist" );
            }
            
            //check if the new file already exists
            Path path2 = Paths.get( DATABASES_PATH + "/" + database_name + "/" + new_collection_name + ".json" );
            
            if(path2.toFile().exists())
            {
                LoggingService.logError( "File " + new_collection_name + ".json already exists" );
                throw new IllegalArgumentException( "File " + new_collection_name + ".json already exists" );
            }
            
            //rename the file
            path1.toFile().renameTo( new File( DATABASES_PATH + "/" + database_name + "/" + new_collection_name + ".json" ) );
            renameCollectionInSequencingFile( database_name , old_collection_name , new_collection_name );
            
            LoggingService.logInfo( "Collection " + old_collection_name + " renamed to " + new_collection_name );
        }
        finally
        {
            reentrant_lock_1.unlock();
        }
    }
    
    //deleting
    
    public static void deleteDatabase(String DATABASES_PATH , String database_name)
    {
        reentrant_lock_1.lock();
        
        try
        {
            
            database_name = database_name.toLowerCase();
            
            Path path = Paths.get( DATABASES_PATH + "/" + database_name );
            
            //check if the directory exists
            if(!path.toFile().exists())
            {
                LoggingService.logError( "Directory " + database_name + " doesn't exist" );
                throw new IllegalArgumentException( "Directory " + database_name + " doesn't exist" );
            }
            
            //delete the directory and all its content
            try
            {
                deleteDirectory( path );
            }
            catch(IOException e)
            {
                e.printStackTrace();
            }
            deleteDatabaseInSequencingFile( database_name );
        }
        finally
        {
            reentrant_lock_1.unlock();
        }
    }
    
    public static void deleteDirectory(Path path) throws IOException
    {
        reentrant_lock_1.lock();
        
        try
        {
            Files.walkFileTree( path , new SimpleFileVisitor<Path>()
            {
                @Override
                public FileVisitResult visitFile(Path file , BasicFileAttributes attrs)
                {
                    try
                    {
                        Files.delete( file );
                    }
                    catch(IOException e)
                    {
                        e.printStackTrace();
                    }
                    return FileVisitResult.CONTINUE;
                }
                
                @Override
                public FileVisitResult postVisitDirectory(Path dir , IOException exc)
                {
                    File file = new File( dir.toString() );
                    file.delete();
                    return FileVisitResult.CONTINUE;
                }
            } );
        }
        finally
        {
            reentrant_lock_1.unlock();
        }
    }
    
    public static void deleteCollection(String DATABASES_PATH , String database_name , String collection_name)
    {
        reentrant_lock_1.lock();
        
        try
        {
            
            database_name = database_name.toLowerCase();
            
            collection_name = collection_name.toLowerCase();
            
            Path path = Paths.get( DATABASES_PATH + "/" + database_name + "/" + collection_name + ".json" );
            
            //check if the file exists
            if(!path.toFile().exists())
            {
                LoggingService.logError( "path to file: " + path + " doesn't exist" );
                throw new IllegalArgumentException( "path to file: " + path + " doesn't exist" );
            }
            
            //delete the file
            try
            {
                Files.delete( path );
                LoggingService.logInfo( "File " + collection_name + ".json deleted" );
            }
            catch(IOException e)
            {
                e.printStackTrace();
            }
            deleteCollectionInSequencingFile( database_name , collection_name );
        }
        finally
        {
            reentrant_lock_1.unlock();
        }
    }
    
    public static void deleteDocument(String DATABASES_PATH , String database_name , String collection_name , long document_id)
    {
        reentrant_lock_1.lock();
        
        try
        {
            
            database_name = database_name.toLowerCase();
            
            collection_name = collection_name.toLowerCase();
            
            Path path = Paths.get( DATABASES_PATH + "/" + database_name + "/" + collection_name + ".json" );
            
            //check if the file exists
            if(!path.toFile().exists())
            {
                LoggingService.logError( "File " + collection_name + ".json doesn't exist" );
                throw new IllegalArgumentException( "File " + collection_name + ".json doesn't exist" );
            }
            
            //read the file
            String json_string = "";
            try
            {
                json_string = new String( Files.readAllBytes( path ) );
            }
            catch(IOException e)
            {
                e.printStackTrace();
            }
            
            //parse the json
            JSONArray json_array = new JSONArray( json_string );
            
            //check if the document exists in the array
            for(int i = 0 ; i < json_array.length() ; i++)
            {
                JSONObject json_object = json_array.getJSONObject( i );
                
                if(json_object.get( "id" ).toString().equals( String.valueOf( document_id ) ))
                {
                    //delete the document
                    json_array.remove( i );
                    break;
                }
            }
            
            //write the file
            try
            {
                Files.write( path , json_array.toString().getBytes() );
            }
            catch(IOException e)
            {
                e.printStackTrace();
            }
            
            LoggingService.logInfo( "Document " + document_id + " deleted" );
        }
        
        finally
        {
            reentrant_lock_1.unlock();
        }
    }
    
    public static void deleteProperty(String DATABASES_PATH , String database_name , String collection_name , long document_id , String property_name)
    {
        reentrant_lock_1.lock();
        
        try
        {
            
            database_name = database_name.toLowerCase();
            
            collection_name = collection_name.toLowerCase();
            
            property_name = property_name.toLowerCase();
            
            Path path = Paths.get( DATABASES_PATH + "/" + database_name + "/" + collection_name + ".json" );
            
            //check if the file exists
            if(!path.toFile().exists())
            {
                LoggingService.logInfo( "File " + collection_name + ".json doesn't exist" );
                throw new IllegalArgumentException( "File " + collection_name + ".json doesn't exist" );
            }
            
            //read the file
            String json_string = "";
            try
            {
                json_string = new String( Files.readAllBytes( path ) );
            }
            catch(IOException e)
            {
                e.printStackTrace();
            }
            
            //parse the json
            JSONArray json_array = new JSONArray( json_string );
            
            //check if the document exists in the array
            for(int i = 0 ; i < json_array.length() ; i++)
            {
                JSONObject json_object = json_array.getJSONObject( i );
                
                if(json_object.get( "id" ).toString().equals( String.valueOf( document_id ) ) && json_object.has( property_name ))
                {
                    //delete the property
                    json_object.remove( property_name );
                    break;
                }
            }
            
            //write the file
            try
            {
                Files.write( path , json_array.toString().getBytes() );
            }
            catch(IOException e)
            {
                e.printStackTrace();
            }
            
            LoggingService.logInfo( "Property " + property_name + " was deleted from document with id = " + document_id );
        }
        finally
        {
            reentrant_lock_1.unlock();
        }
    }
}
