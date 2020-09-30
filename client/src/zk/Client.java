package zk;
import org.I0Itec.zkclient.ZkClient;
import java.io.ObjectOutputStream;
import java.io.ByteArrayOutputStream;
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
	
	public Client(ZkClient zkClient) {
		this.zkClient = zkClient;
		this.jobNum = 1;
	}

	public void runClient()throws InterruptedException,IOException{
		
		//connect to the zk and upload the parameter as a string separated by ',' to /prj/job1
		for(int j = 1; j <= 3; j++) {
			System.out.println("Job" + jobNum + " is submitting");
			jobNum = submitJob(jobNum);
			//System.out.println("Job" + jobNum + " is waiting");
			//Thread.sleep(10000);
		}
		Thread.sleep(10000);
		jobNum = this.submitJob(jobNum);
		Thread.sleep(100000000);
	
	}
	
	public int submitJob (int jobNum) {
		int[] defaultPara = {10000, 20000, 30000};
		String strPara = "";
		zkClient.createPersistent("/prj/job" + jobNum);
		zkClient.createPersistent("/prj/job" + jobNum +"-result");
		Job job = new Job(jobNum,defaultPara);
		/*byte[] bt = null;
		ByteArrayOutputStream bs = new ByteArrayOutputStream();
		try{
			ObjectOutputStream os = new ObjectOutputStream(bs);
			os.writeObject(job);
			os.flush();
			bt = bs.toByteArray();
			bs.close();
			os.close();
		}catch(Exception e){
		}
		*/
		zkClient.writeData("/prj/job"+jobNum, job);
		zkClient.writeData("/prj/job"+jobNum+"-result", "");
		//waiting for job1's result

	    zkClient.subscribeDataChanges("/prj/job"+jobNum + "-result", new DataListener(this,zkClient));
	    jobNum++;
	    return jobNum;
		
		
	}
	

	
	public void announceResult(double result,String job) {
		System.out.println(job +" done;Pi's approximation is "+result);
		
	}
	
}

