import org.junit.*;
import java.util.*;
import java.lang.reflect.Method;
import static org.junit.Assert.*;

import com.sforce.async.*;
import com.sforce.ws.ConnectionException;

public class BulkClientDriverUnitTest {
  private BulkClientDriver driver;

  @Test
  public void testFormatQueryString() {
    driver = new BulkClientDriver(null, null, null, null);
    String expected = "SELECT Id,Name,CreatedDate FROM Account WHERE Rating = 'Hot'";
      assertEquals(expected,
        driver.formatQueryString(new String[]{"Id","Name","CreatedDate"}, 
          "Account", "WHERE Rating = 'Hot'"));
  }

  @Test
  public void testFormatBoundaryQueryString(){
    driver = new BulkClientDriver(null, null, null, null);
    driver.setBulkQueryRule(null, "CreatedDate = '2012-12-13'");
    driver.setChunkingRule("Id", "Account", null, 0, null);

    String threshold = "000000000000000";
    int offset = 249999;

    String expected = "SELECT Id FROM Account " +
                      "WHERE CreatedDate = '2012-12-13' " +
                      "AND Id > '000000000000000' " +
                      "ORDER BY Id ASC " +
                      "LIMIT 1 OFFSET 249999";

    assertEquals(expected,
        driver.formatBoundaryQueryString(threshold, offset));
  }

  //@Before
  //public void beforeEach(){
  //  String username = "qiang@statefarm.com.datafull";
  //  String password = "passw0rd1";
  //  String baseAuthEndpoint = "https://cs10.salesforce.com/services/Soap/c";
  //  String apiVersion = "26.0";
  //  driver = new BulkClientDriver(username, password, baseAuthEndpoint, apiVersion);
  //  driver.initializeConnections();
  //  driver.setTestMode(false, 0);
  //}

  //@After
  //public void afterEach(){ driver = null; }

  //@Test
  //public void testGeneratePermEnabledBulkQueries(){
  //  driver.setChunkingRule("ExctractionId__c", "TestObject__c", "000000", 249, null);
  //  driver.setBulkQueryRule(new String[]{"Id","ExctractionId__c"}, null);
  //  ArrayList<String> boundaries = driver.lookUpBoundaries();
  //  System.out.println(boundaries);
  //  LinkedList<String> queries = driver.generatePermEnabledBulkQueries(boundaries);
  //  System.out.println(queries);

  //  //driver.setChunkingRule("Id", "TestObject__c", null, null);
  //  //driver.setBulkQueryRule(new String[]{"Id"}, null);
  //  //ArrayList<String> fakeBoundaries = new ArrayList<String>();
  //  //fakeBoundaries.add("00000000");
  //  //fakeBoundaries.add("00002500");
  //  //fakeBoundaries.add("00005000");
  //  //fakeBoundaries.add("00007500");
  //  //fakeBoundaries.add("less");
  //  //fakeBoundaries.add("more");
  //  //System.out.println(driver.generatePermEnabledBulkQueries(fakeBoundaries));
  //}

  //@Test
  //public void lookUpBoundaries(){
  //  driver.setChunkingRule("ExctractionId__c", "TestObject__c", "000000", 249, null);
  //  System.out.println(driver.lookUpBoundaries());
  //}


  //@Test
  //public void testSetUpSOAPConnection(){
  //  assertNotNull(driver.getSessionId());
  //  assertNotNull(driver.getEnterpriseConnection());
  //}

  //@Test
  //public void testCreateJob() throws ConnectionException, AsyncApiException{
  //  JobInfo job = driver.createJob("Account", OperationEnum.query, ConcurrencyMode.Parallel);
  //  assertNotNull(job.getId());
  //  //tear down
  //  driver.closeJob(job.getId());
  //}

  //@Test
  //public void testGenerateBulkQueries(){
  //  driver.setChunkingRule("TestAutoNum__c", "TestObject__c", null, 249, null);
  //  driver.setBulkQueryRule(new String[]{"Id", "TestAutoNum__c"}, "CreatedDate = 1987-10-13");
  //  for(String str: driver.generateBulkQueries(0, 999)){
  //    System.out.println(str);
  //  }
  //}

  //@Test
  //public void testGetMinMaxChunk(){
  //  String boundaryField  = "TestAutoNum__c";
  //  String objectType     = "TestObject__c";
  //  HashMap<String, String> hm = new HashMap<String, String>();
  //  hm.put("LB", "ORDER BY " + boundaryField + " ASC NULLS LAST LIMIT 1");
  //  hm.put("UB", "ORDER BY " + boundaryField + " DESC NULLS LAST LIMIT 1");

  //  driver.setChunkingRule(boundaryField, objectType, null, 249, hm);
  //  int lowerBound = driver.getMinMaxChunk("LB");
  //  int upperBound = driver.getMinMaxChunk("UB");
  //  assertEquals(0, lowerBound);
  //  assertEquals(999, upperBound);
  //}

  //@Test public void testJavaReflection() throws ClassNotFoundException,
  //  ConnectionException, IllegalAccessException, java.lang.reflect.InvocationTargetException
  //{
  //  com.sforce.soap.enterprise.QueryResult queryResult =
  //    driver.getEnterpriseConnection().query("SELECT Id, Name FROM Account LIMIT 5");
  //  Class cls = Class.forName("com.sforce.soap.enterprise.sobject.Account");
  //  Object obj = cls.cast(queryResult.getRecords()[0]);
  //  for(Method m: cls.getDeclaredMethods()){
  //    if(m.getName().startsWith("getName")){
  //      System.out.println(m.invoke(obj));
  //    }
  //  }
  //}

  //import org.apache.commons.lang3.time.StopWatch;
  //@Test public void testGetElapsedTime() throws InterruptedException{
  //  String expected = "0:00:03.345";
  //  StopWatch sw = new StopWatch();
  //  sw.start();
  //  Thread.sleep(3345);
  //  sw.stop();
  //  System.err.println(driver.getElapsedTime(sw));
  //}

  //@Test
  //public void testQueryResultObjectBehavior() throws ConnectionException{
  //  String q1 = "SELECT Id FROM Account WHERE Name = 'non-existent' LIMIT 1";
  //  String q2 = "SELECT Id FROM Account WHERE Name = 'United Oil & Gas Corp.' LIMIT 1";
  //  com.sforce.soap.enterprise.QueryResult qr = driver.getEnterpriseConnection().query(q1);
  //  assertEquals(0, qr.getSize());
  //  qr = driver.getEnterpriseConnection().query(q2);
  //  assertEquals("001d000000CMg1GAAT", qr.getRecords()[0].getId());
  //}
}
