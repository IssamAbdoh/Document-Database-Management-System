package slave.sharedpackages.exporting;


import slave.sharedpackages.rwoperations.DatabaseReadService;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Objects;

public class ExportService
{
    private ExportService()
    {
    
    }
    
    public static void exportSchema(String EXPORT_DIRECTORY_PATH , String DATABASES_PATH , String database_name , String schema_name)
    {
        if(DatabaseReadService.isDatabaseExists( DATABASES_PATH , database_name ))
        {
            StringBuilder sb = new StringBuilder();
            sb.append( database_name );
            sb.append( "\n" );
            Objects.requireNonNull( DatabaseReadService.getAllCollectionNames( DATABASES_PATH , database_name ) ).forEach( collection_name ->
            {
                sb.append( collection_name );
                sb.append( "\n" );
            } );
            
            Path path = Paths.get( EXPORT_DIRECTORY_PATH + "/" + schema_name + ".dds" );
            createSchema( EXPORT_DIRECTORY_PATH , schema_name , sb , path );
        }
    }
    
    private static void createSchema(String EXPORT_DIRECTORY_PATH , String schema_name , StringBuilder sb , Path path)
    {
        int version = 0;
        while(path.toFile().exists())
        {
            version++;
            path = Paths.get( EXPORT_DIRECTORY_PATH + "/" + schema_name + "(" + version + ").dds" );
        }
        
        try
        {
            path.toFile().createNewFile();
            Files.write( path , sb.toString().getBytes() );
        }
        catch(IOException e)
        {
            e.printStackTrace();
        }
    }
    
    public static void exportSchemaTo(String DATABASES_PATH , String database_name , String schema_name , String path)
    {
        if(DatabaseReadService.isDatabaseExists( DATABASES_PATH , database_name ))
        {
            StringBuilder sb = new StringBuilder();
            sb.append( database_name );
            sb.append( "\n" );
            Objects.requireNonNull( DatabaseReadService.getAllCollectionNames( DATABASES_PATH , database_name ) ).forEach( collection_name ->
            {
                sb.append( collection_name );
                sb.append( "\n" );
            } );
            
            Path path1 = Paths.get( path );
            createSchema( path , schema_name , sb , path1 );
        }
    }
}
