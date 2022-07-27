package master.control;


import master.indexing.IndexGeneratingService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cloud.client.discovery.DiscoveryClient;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.client.RestTemplate;

@RestController
@RequestMapping (value = "/master/read")
public class ReadController
{
    private final RestTemplate rest_template;
    final DiscoveryClient discovery_client;
    IndexGeneratingService index_generating_service;
    
    @Autowired
    public ReadController(RestTemplate rest_template , DiscoveryClient discovery_client , IndexGeneratingService index_generating_service)
    {
        this.rest_template = rest_template;
        this.discovery_client = discovery_client;
        this.index_generating_service = index_generating_service;
    }
    
    @GetMapping (value = "/{database}/{collection}/document")//take id RequestParam
    public String getDocument(@PathVariable ("database") String database , @PathVariable ("collection") String collection , @RequestParam String id)
    {
        String url = "http://slave/slave/" + database + "/" + collection + "/document";
        return rest_template.getForEntity( url + "?id=" + id , String.class ).getBody();
    }
    
    @GetMapping (value = "/{database}/{collection}/documents/{id}/{field}")
    public String getField(@PathVariable ("database") String database , @PathVariable ("collection") String collection ,
                           @PathVariable ("id") long id , @PathVariable ("field") String field)
    {
        String url = "http://slave/slave/" + database + "/" + collection + "/documents" + "/" + id + "/" + field;
        return rest_template.getForEntity( url , String.class ).getBody();
    }
    
    @GetMapping (value = "/{database}/{collection}")
    public String getCollection(@PathVariable ("database") String database , @PathVariable ("collection") String collection)
    {
        String url = "http://slave/slave/" + database + "/" + collection;
        return rest_template.getForEntity( url , String.class ).getBody();
    }
    
    @GetMapping (value = "/{database}")
    public String getDatabase(@PathVariable ("database") String database)
    {
        String url = "http://slave/slave/" + database;
        return rest_template.getForEntity( url , String.class ).getBody();
    }
    
    @GetMapping (value = "/{database}/collectionsNames")
    public String getAllCollectionNames(@PathVariable ("database") String database)
    {
        String url = "http://slave/slave/" + database + "/collectionsNames";
        return rest_template.getForEntity( url , String.class ).getBody();
    }
    
    @GetMapping (value = "/{database}/{collection}/documents")
    public String getDocuments(@PathVariable ("database") String database , @PathVariable ("collection") String collection ,
                               @RequestParam String field , @RequestParam String value)
    {
        String url = "http://slave/slave/" + database + "/" + collection + "/documents";
        return rest_template.getForEntity( url + "?field=" + field + "&value=" + value , String.class ).getBody();
    }
}
