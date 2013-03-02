import java.io.*;
import java.util.*;
import java.lang.reflect.Method;

import com.sforce.async.*;
import com.sforce.soap.enterprise.EnterpriseConnection;
import com.sforce.soap.enterprise.QueryResult;
import com.sforce.ws.ConnectionException;
import com.sforce.ws.ConnectorConfig;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.StopWatch;

public class BulkClientDriver{
  private String username;
  private String password;
  private String authEndpoint;
  private String apiVersion;

  private String sessionId;
  private BulkConnection bulkConnection;
  private EnterpriseConnection enterpriseConnection;

  // getters for some persistent connection and session
  public String getSessionId(){ return this.sessionId; }
  public BulkConnection getBulkConnection() { return this.bulkConnection; }
  public EnterpriseConnection getEnterpriseConnection(){ return this.enterpriseConnection; }

  public BulkClientDriver(String username, String password, String baseAuthEndpoint, String apiVersion){
    this.username = username;
    this.password = password;
    this.authEndpoint = baseAuthEndpoint + "/" + apiVersion;
    this.apiVersion = apiVersion;
  }

  public void initializeConnections(){
    if(enterpriseConnection == null || bulkConnection == null){
      setUpConnections();
    }
  }

  private void setUpConnections(){
    setUpEnterpriseConnection();  //sync connection
    setUpBulkConnection();        //async connection
  }

  // doBulkQuery do the following in sequence
  // 1. generate queries
  // 2. create a bulk query Job
  // 3. create each batches for the job (enqueue batch jobs)
  // 4. await complete (monitoring)
  public void doBulkQuery(){
    StopWatch sw = new StopWatch();
    try {
      //LinkedList<String> toBeProcessedQueries = generateBulkQueries(getMinMaxChunk("LB"), getMinMaxChunk("UB"));
      LinkedList<String> toBeProcessedQueries = generatePermEnabledBulkQueries(lookUpBoundaries());

      debug("Bulk queries created.\n");
      JobInfo job = createJob(this.objectType, OperationEnum.query, ConcurrencyMode.Parallel);
      debug("Bulk load job created.\n");

      sw.start();
      List<BatchInfo> batchInfos = runBulkQueries(job, toBeProcessedQueries);
      sw.stop();
      closeJob(job.getId());

      debug("All bulk queries enqueued.");
      debug("Time used to enqueue all the jobs: " + sw.toString());

      debug("\nWait completion...\n");
      awaitCompletion(job, batchInfos);
      debug("\nComplete!\n");

      debug("\nDownload batch query result into csv files\n");
      writeResultToFile(job, batchInfos);
      debug("\nDonwload complete!\n");
    } catch(Exception e){
      e.printStackTrace();
    }
  }

  public List<BatchInfo> runBulkQueries(JobInfo job, LinkedList<String> queries) 
    throws AsyncApiException, InterruptedException
  {
    List<BatchInfo> batchInfos = new ArrayList<BatchInfo>();
    // this loop runs about 27 hours
    for (int i=0; i < 10000; i++){
      debug("Waking up, checking...");
      job = this.bulkConnection.getJobStatus(job.getId());
      int numOfQueuedJob = job.getNumberBatchesQueued() + job.getNumberBatchesInProgress();
      debug("Jobs currently in queue: " + numOfQueuedJob);
      // if number of queued and running jobs is more than 10, stop adding new jobs
      while(numOfQueuedJob < 10){
        if(queries.size() == 0)
          return batchInfos;

        String query = queries.removeFirst();
        ByteArrayInputStream bout = new ByteArrayInputStream(query.getBytes());
        batchInfos.add(
            this.bulkConnection.createBatchFromStream(job, bout));
        numOfQueuedJob++;
      }
      Thread.sleep(10*1000);
    }
    return batchInfos;
  }

  // The following variables configure all the fields that bulk query should retrieve
  private String[] bulkQueryFields;
  private String   bulkQueryCondition;

  public void setBulkQueryRule(String[] bulkQueryFields, String bulkQueryCondition){
    this.bulkQueryFields = bulkQueryFields;
    this.bulkQueryCondition = bulkQueryCondition;
  }

  // generate chunked bulk queries
  public LinkedList<String> generateBulkQueries(int min, int max){
    LinkedList<String> toBeProcessedQueries = new LinkedList<String>();

    debug("Min: " + min + " | Max: " + max );

    int noOfChunk;
    if(this.isTest){
      noOfChunk = this.numberOfChunks;
    } else {
      noOfChunk = Double.valueOf(Math.ceil((max - min)/this.chunkSize)).intValue();
    }
    debug("Total chunks:" + noOfChunk);

    int lowerBound = min - 1;
    int upperBound = min + this.chunkSize;

    String queryString;
    for(int i=0; i < noOfChunk; i++){
      // for the last chunk, set upperBound to max
      upperBound = (i == noOfChunk-1 ? max : upperBound);

      queryString = formatQueryString(this.bulkQueryFields, this.objectType,
          "WHERE " + (StringUtils.isBlank(this.bulkQueryCondition) ? "" :
                        "AND " + this.bulkQueryCondition + " ") +
          this.boundaryField + " > " + lowerBound + " AND " +
          this.boundaryField + " <= " + upperBound);
      debug("Generated query " + (i+1) + ": [" + queryString + "]\n");
      toBeProcessedQueries.add(queryString);

      lowerBound  = upperBound;
      upperBound += this.chunkSize;
    }

    return toBeProcessedQueries;
  }

  // Scenario where Salesforce allows "Id" field to be used as boundaries
  // which means "Id" is sequencial
  public LinkedList<String> generatePermEnabledBulkQueries(ArrayList<String> boundaries){
    LinkedList<String> toBeProcessedQueries = new LinkedList<String>();

    String queryString;
    String lowerBound = "";
    String upperBound = "";

    //System.out.println(this.bulkQueryFields);
    for (int i=0; i < boundaries.size()-2; i++){
      lowerBound = boundaries.get(i);
      upperBound = boundaries.get(i + 1);
      queryString = formatQueryString(this.bulkQueryFields, this.objectType,
          "WHERE " + (StringUtils.isBlank(this.bulkQueryCondition) ? "" :
                        "AND " + this.bulkQueryCondition + " ") +
          this.boundaryField + " > '" + lowerBound + "' AND " +
          this.boundaryField + " <= '" + upperBound + "'");
      debug("\nGenerated query " + (i+1) + ": [" + queryString + "]");
      toBeProcessedQueries.add(queryString);
    }

    if(boundaries.get(boundaries.size()-1) == "more"){
      queryString = formatQueryString(this.bulkQueryFields, this.objectType,
          "WHERE " + (StringUtils.isBlank(this.bulkQueryCondition) ? "" :
                        "AND " + this.bulkQueryCondition + " ") +
          this.boundaryField + " > '" + upperBound + "'"
      );
      debug("\nGenerated last query : [" + queryString + "]\n");
      toBeProcessedQueries.add(queryString);
    }

    return toBeProcessedQueries;
  }

  // the following variable configs the test mode
  // they are set by setTestMode() method
  private Boolean isTest;
  private Integer numberOfChunks;

  public void setTestMode(Boolean isTest, Integer numberOfChunks){
    this.isTest = isTest;
    this.numberOfChunks = numberOfChunks;
  }

  // the following three variable are used by constructing chunking query
  // they are set by setChunkingRule() method
  private int chunkSize;
  private String objectType;
  private String boundaryField;
  private String initialChunkingThreshold;
  private Map<String, String> chunkingConditions;

  public void setChunkingRule(String boundaryField, String objectType,
      String initialChunkingThreshold, int chunkSize, Map<String, String> conditions)
  {
    this.objectType = objectType;
    this.boundaryField = boundaryField;
    this.initialChunkingThreshold = initialChunkingThreshold;
    this.chunkSize = chunkSize;
    this.chunkingConditions = conditions;
  }

  public int getMinMaxChunk(String boundary){
    QueryResult queryResults;
    Integer range = 0;
    try {
      queryResults = this.enterpriseConnection.query(
        formatQueryString(
          new String[]{ this.boundaryField }, this.objectType, this.chunkingConditions.get(boundary)
        )
      );

      // some reflection magic
      Class cls = Class.forName("com.sforce.soap.enterprise.sobject." + this.objectType);
      Object rec = cls.cast(queryResults.getRecords()[0]);
      for(Method m: cls.getDeclaredMethods()){
        if(m.getName().startsWith("get" + StringUtils.capitalize(this.boundaryField))){
          range = ((Double)m.invoke(rec)).intValue();
          break;
        }
      }
    } catch (Exception e){
      // possible exceptions
      // 1. com.sfroce.ws.ConnectionException
      // 2. java.lang.reflect.ClassNotFoundException
      // 3. java.lang.reflect.IllegalAccessException
      // 4. java.lang.reflect.InvocationTargetException
      e.printStackTrace();
    }
    return range;
  }

  // this only works with permission enabled salesforce instance
  // for now, boundaryField can only be "Id", it can not be null
  public ArrayList<String> lookUpBoundaries(){
    ArrayList<String> boundaries = new ArrayList<String>();
    QueryResult queryResults;
    String threshold = this.initialChunkingThreshold;
    boundaries.add(threshold);
    int offset = this.chunkSize;
    try{
      int c = 0;
      while(this.isTest ? c < this.numberOfChunks : true){
        System.out.println(formatBoundaryQueryString(threshold, offset));
        queryResults = this.enterpriseConnection.query(
                         formatBoundaryQueryString(threshold, offset));
        if(queryResults.getSize() == 0){
          //need to do another query to find out if there are more record
          queryResults = this.enterpriseConnection.query(
                          formatBoundaryQueryString(threshold, 0));
          if(queryResults.getSize() > 0){
            boundaries.add("more");
          } else {
            boundaries.add("less");
          }
          break;
        } else {
          threshold = queryResults.getRecords()[0].getId();
          boundaries.add(threshold);
        }
        c++;
      }
    } catch (Exception e){
      e.printStackTrace();
    }
    return boundaries;
  }

  private void setUpEnterpriseConnection(){
    ConnectorConfig enterpriseConfig = new ConnectorConfig();
    enterpriseConfig.setUsername(this.username);
    enterpriseConfig.setPassword(this.password);
    enterpriseConfig.setAuthEndpoint(this.authEndpoint);
    try {
      this.enterpriseConnection = new EnterpriseConnection(enterpriseConfig);
      this.sessionId = enterpriseConfig.getSessionId();
    } catch(ConnectionException e){
      e.printStackTrace();
    }
  }

  private void setUpBulkConnection(){
    if(this.sessionId == null){
      setUpEnterpriseConnection();
      setUpBulkConnection();// bulkConnection depends on session id
    } else {
      ConnectorConfig config = new ConnectorConfig();
      config.setSessionId(this.sessionId);
      config.setRestEndpoint(this.authEndpoint.substring(0, this.authEndpoint.indexOf("Soap/")) 
          + "async/" + this.apiVersion);
      // set compression to false, tracemessage to true when debugging 
      config.setCompression(true);
      config.setTraceMessage(false);
      try {
        BulkConnection connection = new BulkConnection(config);
        this.bulkConnection = connection;
      } catch (AsyncApiException e){
        e.printStackTrace();
      }
    }
  }

  public JobInfo createJob(String sobjectType, OperationEnum operationType,
      ConcurrencyMode conMode) throws AsyncApiException
  {
    JobInfo job = new JobInfo();
    job.setObject(sobjectType);
    job.setOperation(operationType);
    job.setConcurrencyMode(conMode);
    job.setContentType(ContentType.CSV);
    job = this.bulkConnection.createJob(job);
    return job;
  }

  public void closeJob(String jobId) throws AsyncApiException {
    JobInfo job = new JobInfo();
    job.setId(jobId);
    job.setState(JobStateEnum.Closed);
    this.bulkConnection.updateJob(job);
  }

  public void awaitCompletion(JobInfo job, List<BatchInfo> batchInfoList)
    throws AsyncApiException
  {
    long sleepTime = 0L;
    Set<String> incomplete = new HashSet<String>();
    for (BatchInfo bi : batchInfoList) {
      incomplete.add(bi.getId());
    }
    while (!incomplete.isEmpty()) {
      try {
        Thread.sleep(sleepTime);
      } catch (InterruptedException e) {}

      debug("Awaiting results... incomplete size: " + incomplete.size());

      sleepTime = 10000L;
      BatchInfo[] statusList = bulkConnection.getBatchInfoList(job.getId()).getBatchInfo();
      for (BatchInfo b : statusList) {
        if (b.getState() == BatchStateEnum.Completed || b.getState() == BatchStateEnum.Failed){
          if (incomplete.remove(b.getId())) {
            debug("\nBATCH STATUS:\n" + b);
          }
        }
      }
    }
  }

  public String formatBoundaryQueryString(String threshold, int offset){
      return formatQueryString( new String[]{ this.boundaryField }, this.objectType,
        "WHERE " + (StringUtils.isBlank(this.bulkQueryCondition) ? "" :
                      "AND " + this.bulkQueryCondition + " ") +
        this.boundaryField + " > '" + threshold + "'" +
        " ORDER BY " + this.boundaryField + " ASC " +
        " LIMIT 1 OFFSET " + offset
      );
  }

  public String formatQueryString(String[] selectString, String fromString, 
      String conditionString)
  {
    return String.format("SELECT %s FROM %s %s",
        StringUtils.join(selectString, ","), fromString, conditionString);
  }

  // These configs files generated by bulk query
  private String baseDir;
  private String outputSummaryFileName;
  private String outputRecordsFileNamePrefix;
  private Boolean concatenate;

  public void setBulkQueryOutputFileConfig(String base_dir, String outputSummaryFileName,
    String outputRecordsFileNamePrefix, Boolean concatenate)
  {
    this.baseDir = baseDir;
    this.outputSummaryFileName      = outputSummaryFileName;
    this.outputRecordsFileNamePrefix = outputRecordsFileNamePrefix;
    this.concatenate = concatenate;
  }

  private File createCSVFile(String uid) throws IOException{
    File csv = new File("data", this.outputRecordsFileNamePrefix + uid  + ".csv");
    if(!csv.exists()){
      csv.createNewFile();
    }
    return csv;
  }

  public void writeResultToFile(JobInfo job, List<BatchInfo> batchInfoList)
    throws AsyncApiException, IOException
  {
    List<String> filenames = new ArrayList<String>();
    for (BatchInfo b : batchInfoList) {
      File csv = createCSVFile(b.getId());
      filenames.add(csv.getAbsolutePath());
      FileOutputStream out = new FileOutputStream(csv);
      String[] results = bulkConnection.
                          getQueryResultList(job.getId(), b.getId()).getResult();

      InputStream in = this.bulkConnection.
                         getQueryResultStream(job.getId(), b.getId(), results[0]);

      byte[] bytes = new byte[1024];
      int read = 0;
      while ((read = in.read(bytes)) != -1) {
        out.write(bytes, 0, read);
      }
      in.close();
      out.flush();
      out.close();
    }
    // optional step, concatenate all
    //System.out.println(StringUtils.join(filenames, ", "));
    if(this.concatenate)
      concatenate(filenames);
  }

  private void concatenate(List<String> files) throws IOException{
    FileOutputStream out = new FileOutputStream(createCSVFile("all"));
    for(String filename: files){
      BufferedReader in = new BufferedReader(
                            new InputStreamReader(
                              new FileInputStream(new File(filename))));
      in.readLine(); //skip
      String line;
      while((line=in.readLine()) != null){
        line += "\n";
        out.write(line.getBytes());
      }
      out.flush();
      in.close();
    }
    out.close();
  }

  public void debug(String message){ System.out.println(message); }
}
