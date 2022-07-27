package com.eurekaserver;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.netflix.eureka.server.EnableEurekaServer;

@EnableEurekaServer
@SpringBootApplication
public class EurekaServerApplication
{
    public static void main(String[] args)
    {
        //--server.port=8761
        //System.setProperty( "server.port" , "8761" );
        
        SpringApplication.run( EurekaServerApplication.class , args );
    }
}
