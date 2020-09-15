package zk;
import org.I0Itec.zkclient.ZkClient;
import java.util.LinkedList;
import org.apache.zookeeper.*;
import java.io.IOException;
import org.apache.log4j.PropertyConfigurator;
import java.util.Queue;
import java.io.IOException;
import java.util.LinkedList;
import java.util.Queue;

public class Client {
	static ZkClient zkClient;
	public int jobNum;

	public void runClient()throws InterruptedException,IOException{
		Thread.sleep(10000);
		jobNum = 1;
		
		//connect to the zk and upload the parameter as a string separated by ',' to /prj/job1
		zkClient= new ZkClient("127.0.0.1:2181",1000000);
		for(int j = 1; j <= 3; j++) {
			System.out.println("Job" + jobNum + " is submitting");
			jobNum = this.submitJob(jobNum);
			//System.out.println("Job" + jobNum + " is waiting");
			//Thread.sleep(10000);
		}
		Thread.sleep(10000);
		jobNum = this.submitJob(jobNum);
	
	}
	
	public int submitJob(int jobNum) {
		Object syn = new Object();
		int[] defaultPara = {10000, 20000, 30000};
		String strPara = "";
		zkClient.createPersistent("/prj/job" + jobNum);
		zkClient.createPersistent("/prj/job" + jobNum +"-result");
		for(int i : defaultPara) {
			strPara += i + ",";
		}
		strPara = strPara.substring(0, strPara.length() - 1);
		zkClient.writeData("/prj/job"+jobNum, strPara);
		zkClient.writeData("/prj/job"+jobNum+"-result", "");
		//waiting for job1's result

		Queue<String> jobsDone = new LinkedList<String>();
	    zkClient.subscribeDataChanges("/prj/job"+jobNum + "-result", new DataListener(this,false,zkClient,syn,jobsDone));
	    jobNum++;
	    return jobNum;
		
		
	}
	
	public void announceResult(double result,String job) {
		System.out.println(job +" done;Pi's approximation is "+result);
		
	}
	
}

