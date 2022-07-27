package slave.sharedpackages.exporting;


import slave.sharedpackages.rwoperations.DatabaseReadService;
import slave.sharedpackages.rwoperations.DatabaseWriteService;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class ImportService
{
    private ImportService()
    {
    }
    
    public static void importSchema(String EXPORT_DIRECTORY_PATH , String DATABASES_PATH , String schema_name)
    {
        Path path = Paths.get( EXPORT_DIRECTORY_PATH + "/" + schema_name + ".dds" );
        if(path.toFile().exists())
        {
            convertSchemaToDatabase( DATABASES_PATH , path );
        }
    }
    
    private static void convertSchemaToDatabase(String DATABASES_PATH , Path path)
    {
        try
        {
            String[] lines = new String( Files.readAllBytes( path ) ).split( "\n" );
            String database_name = lines[0];
            if(DatabaseReadService.isDatabaseNotExists( DATABASES_PATH , database_name ))
            {
                //DatabaseWriteOperations.createDatabase( database_name );
                for(int i = 1 ; i < lines.length ; i++)
                {
                    String collection_name = lines[i];
                    DatabaseWriteService.createCollection( DATABASES_PATH , database_name , collection_name );
                }
            }
            else
            {
                throw new IllegalStateException( "Database " + database_name + " already exists\nDelete the " + "database and try again" );
            }
        }
        catch(IOException e)
        {
            e.printStackTrace();
        }
    }
    
    public static void importSchemaFromAPath(String DATABASES_PATH , String schema_path)
    {
        Path path = Paths.get( schema_path );
        if(path.toFile().exists())
        {
            //check if the file is ends with .dds
            if(path.toFile().getName().endsWith( ".dds" ))
            {
                convertSchemaToDatabase( DATABASES_PATH , path );
            }
            else
            {
                throw new IllegalStateException( "The file " + schema_path + " is not a schema file" );
            }
        }
        else
        {
            throw new IllegalStateException( "File " + schema_path + " does not exist" );
        }
    }
}
