import java.util.*;
import java.io.*;
import com.sforce.async.*;

public class DriverMonitor implements Runnable {
  private JobInfo job;
  private Set<String> failedJobs;
  private Set<String> completedJobs;
  private Set<String> notProcessedJobs;
  private BulkConnection connection;

  public DriverMonitor(JobInfo job, BulkConnection connection){
    this.job = job;
    this.connection = connection;
    this.failedJobs = new HashSet<String>();
    this.completedJobs = new HashSet<String>();
    this.notProcessedJobs = new HashSet<String>();
  }

  public void run(){
    BatchInfo[] batchInfos;
    try{
      while(true){
        System.out.println("Checking..." );

        batchInfos = queryBatchInfos();
        for(BatchInfo bi: batchInfos){
          checkAndWriteToFile(bi);
        }
        Thread.sleep(10 * 1000);
      }

    } catch(AsyncApiException e){
      System.out.println("Failed do bulk query!");
    } catch(IOException e){
      System.out.println("Failed to write to file!");
    } catch(InterruptedException e) {
      System.out.println("No more jobs. TODO: write summary to file.");
    }
  }

  // only interested in completed jobs and failed jobs during the processing
  private void checkAndWriteToFile(BatchInfo bi) throws IOException, AsyncApiException{
    if(bi.getState() != BatchStateEnum.Queued
        && bi.getState() != BatchStateEnum.InProgress)
    {
      if(!completedJobs.contains(bi.getId()) && !failedJobs.contains(bi.getId())){
        if (bi.getState() == BatchStateEnum.Completed){
          writeToFile(bi);
          completedJobs.add(bi.getId());
          System.err.println("Batch [" + bi.getId() + "] completed and write to file!");
        } else if (bi.getState() == BatchStateEnum.Failed){
          System.err.println("Batch [" + bi.getId() + "] failed!");
          failedJobs.add(bi.getId());
        } else {
          // do..nothing
        }
      }
    }
  }

  private void writeToFile(BatchInfo bi) throws IOException, AsyncApiException{
    File csv = new File("data", "bulk_query_" + bi.getId() + ".csv");
    csv.createNewFile();
    FileOutputStream out = new FileOutputStream(csv);
    String[] results = this.connection
                          .getQueryResultList(this.job.getId(), bi.getId()).getResult();
    InputStream in = this.connection
                      .getQueryResultStream(this.job.getId(), bi.getId(), results[0]);
    byte[] bytes = new byte[1024];
    int read = 0;
    while((read=in.read(bytes)) != -1){
      out.write(bytes, 0, read);
    }
    in.close();
    out.flush();
    out.close();
  }

  private BatchInfo[] queryBatchInfos() throws AsyncApiException{
    return connection.getBatchInfoList(this.job.getId()).getBatchInfo();
  }
}

