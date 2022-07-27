package scale;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import springfox.documentation.swagger2.annotations.EnableSwagger2;

@SpringBootApplication
@EnableSwagger2
public class ScaleApplication
{
    public static void main(String[] args)
    {
        //change the port number
        System.setProperty( "server.port" , "1234" );
        
        //change application name
        System.setProperty( "spring.application.name" , "scale" );
        
        SpringApplication.run( ScaleApplication.class , args );
    }
}
