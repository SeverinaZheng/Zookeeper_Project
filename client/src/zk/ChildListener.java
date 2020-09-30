package zk;
import org.apache.zookeeper.*;
import org.apache.zookeeper.Watcher;
import java.util.ArrayList;
import org.apache.zookeeper.ZooDefs;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.IZkChildListener;
import java.util.List;
import java.util.Queue;

public class ChildListener implements IZkChildListener {
	Queue<String> newJobs;
	Queue<String> jobsDone;
	Queue<String> vacantWorker ;
	private int childNum;
	
	public ChildListener(Queue newJobs,Queue jobsDone,Queue<String> vacantWorker) {
		this.newJobs = newJobs;
		this.jobsDone = jobsDone;
		this.vacantWorker = vacantWorker;
		this.childNum = 1;
	}
	
	public void handleChildChange(String parentPath, List<String> currentChilds) throws Exception {
		//only add new jobs if new children are added
		if(currentChilds.size() > childNum) {
			childNum = currentChilds.size();
			for(String child : currentChilds) {
	        	if(child.length() < 6 && child.substring(0,3).equalsIgnoreCase("job")) {
	        		if(!newJobs.contains(child) && !jobsDone.contains(child) ) {
	        			newJobs.add(child);
	        		}
	        	}else if(child.substring(0,6).equalsIgnoreCase("worker") && !vacantWorker.contains(child)) {
	        		vacantWorker.add(child);
	        	}
	        	
			}
		}else {
			childNum = currentChilds.size();
			
		}
			
			
        		//System.out.println("New job!");
        		//pmaster.spreadOutJob(child.substring(3,4));
        		//break;
        		//Thread.sleep(5000);
        	/*else if(child.substring(child.length()-6).equalsIgnoreCase("result")) {
        		System.out.println("have result");
        		client.announceResult(zkClient.readData(child));
        	}*/
        
    }

}
