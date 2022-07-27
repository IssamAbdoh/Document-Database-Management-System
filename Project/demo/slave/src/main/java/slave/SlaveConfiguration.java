package slave;

import org.springframework.cloud.client.loadbalancer.LoadBalanced;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.servlet.config.annotation.EnableWebMvc;
import springfox.documentation.builders.PathSelectors;
import springfox.documentation.builders.RequestHandlerSelectors;
import springfox.documentation.spi.DocumentationType;
import springfox.documentation.spring.web.plugins.Docket;

@Configuration
@EnableWebMvc
public class SlaveConfiguration
{
    @Bean
    @LoadBalanced
    public RestTemplate restTemplate()
    {
        return new RestTemplate();
    }
    
    @Bean
    public Docket api()
    {
        return new Docket( DocumentationType.SWAGGER_2 ).select().apis( RequestHandlerSelectors.basePackage( "slave" ) ).apis( RequestHandlerSelectors.any() ).paths( PathSelectors.any() ).build();
    }
}
