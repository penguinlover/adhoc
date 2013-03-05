import java.util.*;
import java.io.FileReader;
import java.io.FileNotFoundException;
import com.esotericsoftware.yamlbeans.YamlReader;
import com.esotericsoftware.yamlbeans.YamlException;

public class DriverRunner{

  @SuppressWarnings("unchecked")
  public static void main (String [] args) throws FileNotFoundException, YamlException {
    YamlReader yr = new YamlReader(new FileReader("config.yml"));
    Map localConfig = (Map)yr.read();

    // config the driver
    BulkClientDriver driver = new BulkClientDriver(
                                      (String)localConfig.get("username"),
                                      (String)localConfig.get("password"),
                                      (String)localConfig.get("baseAuthEndpoint"),
                                      (String)localConfig.get("apiVersion"));

    driver.setChunkingRule(
            (String)localConfig.get("boundaryField"),
            (String)localConfig.get("sobjectType"),
            (String)localConfig.get("initialChunkingThreshold"),
            Integer.valueOf((String)localConfig.get("chunkSize")), null);

    driver.setBulkQueryRule(
        ((ArrayList<String>)localConfig.get("bulkQueryFields")).toArray(new String[0]),
        (String)localConfig.get("bulkQueryCondition"));

    driver.setTestMode(
        Boolean.valueOf((String)localConfig.get("isTest")),
        Integer.valueOf((String)localConfig.get("numberOfChunks")));

    driver.setBulkQueryOutputFileConfig(
        (String)localConfig.get("baseDir"),
        (String)localConfig.get("outputSummaryFileName"),
        (String)localConfig.get("outputRecordsFileNamePrefix"),
        Boolean.valueOf((String)localConfig.get("concatenate")));

    driver.initializeConnections();

    //driver.doBulkQuery();
    driver.doPipelinedBulkQuery();

    //try{
    //  System.out.println(driver.getEnterpriseConnection().queryAll("SELECT Id,ClientIdentifier__c FROM Account WHERE Id > '001J000000HOjwGIAT' AND Id <= '001J000000HcQipIAF'"));

    //  System.out.println(driver.getEnterpriseConnection().query("SELECT Id, IsDeleted FROM Account WHERE Id = '001J000000HcQipIAF'"));
    //} catch (Exception e){
    //  e.printStackTrace();
    //}
    //ArrayList<String> boundaries = driver.lookUpBoundaries();
    //System.out.println(boundaries);
    //LinkedList<String> queries = driver.generatePermEnabledBulkQueries(boundaries);
    //System.out.println(queries);
  }

}
