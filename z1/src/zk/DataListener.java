package zk;
import org.apache.zookeeper.*;
import org.apache.zookeeper.Watcher;
import java.util.ArrayList;
import java.util.LinkedList;

import org.apache.zookeeper.ZooDefs;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.IZkDataListener;
import java.util.List;
import java.util.Queue;

public class DataListener implements IZkDataListener {
	Queue<String> para;
	ZkClient zkClient;
	Queue<String> jobsDone;
	Queue<String> vacantWorker ;
	Queue<String> busyWorker;
	Process p;
	Queue<String> masterDie;
	int serverNumber;
	public DataListener(ZkClient zkClient, Process p,Queue<String> die) {
		this.p = p;
		this.zkClient = zkClient;
		this.masterDie = die;
	}
	
	public DataListener(int serverNumber,Queue<String> para,ZkClient zkClient,Queue<String> jobsDone) {
		this.serverNumber = serverNumber;
		this.para = para;
		this.zkClient = zkClient;
		this.jobsDone = jobsDone;
	}
	
	public DataListener(ZkClient zkClient,Queue<String> jobsDone,Queue<String> vacantWorker,Queue<String> busyWorker) {
		this.zkClient = zkClient;
		this.jobsDone = jobsDone;
		this.vacantWorker = vacantWorker;
		this.busyWorker = busyWorker;
	}
	
	public void handleDataChange(String parentPath, Object o) throws Exception {
		//if there's an update in /prj/jobx-result node, then the master knows a job is done
		//it unsubscribe the node and delete all nodes about jobx
		if(parentPath.substring(parentPath.length()-6).equalsIgnoreCase("result")){
			String data = (String) o;
	        String[] dataArr= data.split(",");
	        String para = zkClient.readData(parentPath.substring(0,9));
	        String[] paraArr = para.split(",");
	        //check if the result node get all results for all parameters
	        if(dataArr.length == paraArr.length){
					jobsDone.add(parentPath.substring(5, 9));
					zkClient.unsubscribeDataChanges(parentPath, this);
	        }
		}else if(parentPath.substring(5,11).equalsIgnoreCase("worker")) {
			//if there's new parameters write in the /prj/worker node 
			//then there's new job
			//by adding to 'para' queue, we inform the worker to start calculating
			//System.out.println("here");
			if(o instanceof String &&((String)o).equals("die")){
				if(vacantWorker.contains(parentPath.substring(5))) 
					vacantWorker.remove(parentPath.substring(5));
				if(busyWorker.contains(parentPath.substring(5))) 
					busyWorker.remove(parentPath.substring(5));
				zkClient.delete(parentPath.substring(5));
			}else if (o instanceof String && ((String)o).equals("")) {
				//a worker has done its job and alive
				busyWorker.remove(parentPath.substring(5));
				vacantWorker.add(parentPath.substring(5));
			
			}else {
				Job job = (Job)zkClient.readData(parentPath);
				//System.out.println("jobAndPara"+jobAndPara);
				para.add("job"+job.jobNum);
				if(job != null) {
					double pi =job.calculate();
					String result = zkClient.readData("/prj/job"+job.jobNum+"-result");
					result +=pi+",";
					zkClient.writeData("/prj/job"+job.jobNum+"-result", result);
					zkClient.writeData(parentPath, "");
					System.out.println(serverNumber + "calculate" + pi + "for "+ job.para[job.paraIndex]);
				}
				
				//after finishing a part of job, the worker may die
				double die = Math.random();
				if(die < 0.2) {
					zkClient.writeData(parentPath, "die");
					System.out.println("worker" + serverNumber + " die");
				}
				
			}
				
		}else if(parentPath.substring(5,11).equalsIgnoreCase("master")){
			LinkedList<String> jobs = new LinkedList<String>();
			jobs = (LinkedList<String>)zkClient.readData(parentPath);
			System.out.println("jobs to do" + jobs.toString());
			if(jobs.peek() != null) {
				String jobNumber =jobs.poll().substring(3,4);
	     		p.spreadOutJob(jobNumber);
	     		Job job= (Job)zkClient.readData("/prj/job"+jobNumber);
	     		int paraLength = job.para.length;
	     		int partitionDone = ((String)zkClient.readData("/prj/job"+jobNumber+"-result")).split(",").length;
	     		while(paraLength != partitionDone) {
						try{Thread.sleep(1);}catch(Exception e) {}
		        		partitionDone = ((String)zkClient.readData("/prj/job"+jobNumber+"-result")).split(",").length;
	     			
	     		}
	     		
	     		double die = Math.random();
				if(die < 0.5) {
						zkClient.delete("/prj/master");
						zkClient.unsubscribeAll();
						System.out.println("master" + " die");
						this.masterDie.add("die");
				}else
					zkClient.writeData("/prj/master", jobs);
			}
			
			
		}
        
    }
	
	public void handleDataDeleted(String parentPath) throws Exception {}

}
