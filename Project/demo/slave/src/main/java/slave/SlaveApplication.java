package slave;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.netflix.eureka.EnableEurekaClient;
import springfox.documentation.swagger2.annotations.EnableSwagger2;

@SpringBootApplication
@EnableSwagger2
@EnableEurekaClient
public class SlaveApplication
{
    public static void main(String[] args)
    {
        //change the port number
        //System.setProperty( "server.port" , "2613" );
        
        //change application name
        //System.setProperty( "spring.application.name" , "slave" );
        
        SpringApplication.run( SlaveApplication.class , args );
    }
}
