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
	
	public DataListener(Queue<String> para,ZkClient zkClient,Queue<String> jobsDone) {
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
		if(parentPath.substring(parentPath.length()-6).equalsIgnoreCase("result")){
			String data = (String) o;
	        String[] dataArr= data.split(",");
	        String para = zkClient.readData(parentPath.substring(0,9));
	        String[] paraArr = para.split(",");
	        if(dataArr.length == paraArr.length){
					jobsDone.add(parentPath.substring(5, 9));
					zkClient.unsubscribeDataChanges(parentPath, this);
					zkClient.deleteRecursive(parentPath);
					zkClient.deleteRecursive(parentPath.substring(0, 9));
	        }
		}else if(parentPath.substring(5,11).equalsIgnoreCase("worker")) {
			if(!((String)o).equals("")) {
				String jobAndPara = zkClient.readData(parentPath);
				//System.out.println("jobAndPara"+jobAndPara);
				para.add(jobAndPara);
			}else {
				busyWorker.remove(parentPath.substring(5,12));
				vacantWorker.add(parentPath.substring(5,12));
			}
				
		}
			
			//double death = Math.random();
			//if(death > 0.5) {
			//	System.out.println("master is dead");
			//}
		
        
    }
	
	public void handleDataDeleted(String parentPath) throws Exception {}

}
