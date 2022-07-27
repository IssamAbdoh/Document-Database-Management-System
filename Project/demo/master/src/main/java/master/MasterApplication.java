package master;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.netflix.eureka.EnableEurekaClient;
import org.springframework.retry.annotation.EnableRetry;
import springfox.documentation.swagger2.annotations.EnableSwagger2;

@SpringBootApplication
@EnableSwagger2
@EnableEurekaClient
@EnableRetry
public class MasterApplication
{
    public static void main(String[] args)
    {
        //Djavaee.jmxremote.authenticate=false
        //change the port number
        //System.setProperty( "server.port" , "2726" );
        //System.setProperty( "server.port" , "2727" );
        
        //change application name
        //System.setProperty( "spring.application.name" , "master" );
        
        SpringApplication.run( MasterApplication.class , args );
        //open http://localhost:2726/swagger-ui.html
    }
}
