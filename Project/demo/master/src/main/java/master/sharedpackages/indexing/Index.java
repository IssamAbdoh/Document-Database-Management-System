package master.sharedpackages.indexing;

import org.json.JSONArray;

public interface Index
{
    static Index fromString(String s)
    {
        return null;
    }
    
    void setPropertyAndPopulate(String property);
    
    void updateCollectionIndex();
    
    JSONArray getDocumentsThatHave(String value);
    
    void setCollectionAndPopulate(String collection);
    
    String getProperty();
    
    String getDatabase();
    
    String getCollection();
    
    Index getIndex();
    
    
}
