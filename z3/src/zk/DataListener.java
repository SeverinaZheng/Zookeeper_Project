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
	int serverNumber;
	
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
				}else {
					System.out.println("parse job not found");
				}
				
				//after finishing a part of job, the worker may die
				double die = Math.random();
				if(die <0.1) {
					zkClient.writeData(parentPath, "die");
					System.out.println("worker" + serverNumber + " die");
				}
				
			}
				
		}
			
			//double death = Math.random();
			//if(death > 0.5) {
			//	System.out.println("master is dead");
			//}
		
        
    }
	
	public void handleDataDeleted(String parentPath) throws Exception {}

}
