package zk;
import org.apache.zookeeper.*;
import java.util.LinkedList;
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
	Queue<String> busyWorker ;
	Queue<String> allJobs ;
	Queue<String> allWorkers = new LinkedList<String>();;
	private int childNum;
	ZkClient zkClient;
	Process p;
	static Queue<String> stop;
	
	public ChildListener(Process p ,Queue newJobs,Queue jobsDone,Queue<String> vacantWorker,Queue<String> busyWorker,ZkClient zkClient) {
		this.p = p;
		this.jobsDone = jobsDone;
		this.newJobs = newJobs;
		this.vacantWorker = vacantWorker;
		this.busyWorker = busyWorker;
		this.childNum = 1;
		this.zkClient = zkClient;
		this.allJobs = new LinkedList<String>();
	}
	public ChildListener(Process p,Queue<String> stop, Queue newJobs,Queue jobsDone,Queue<String> vacantWorker,Queue<String> busyWorker,ZkClient zkClient) {
		this.newJobs = newJobs;
		this.jobsDone = jobsDone;
		this.vacantWorker = vacantWorker;
		this.busyWorker = busyWorker;
		this.childNum = 1;
		this.zkClient = zkClient;
		this.allJobs = new LinkedList<String>();
		this.p = p;
		this.stop = stop;
	}
	
	public void handleChildChange(String parentPath, List<String> currentChilds) throws Exception {
		//only add new jobs if new children are added
		if(currentChilds.size() > childNum) {
			childNum = currentChilds.size();
			int added = 0;
			for(String child : currentChilds) {
	        	if(child.length() < 6 && child.substring(0,3).equalsIgnoreCase("job")) {
	        		//if it's new job
	        		if(!allJobs.contains(child)) {
	        			added++;
	        			newJobs.add(child);
	        			allJobs.add(child);
	         			zkClient.subscribeDataChanges("/prj/"+child+"-result", new DataListener(zkClient,jobsDone,vacantWorker,busyWorker));
	        			
	        		}
	        	}else if(child.length() >5 && child.substring(0,6).equalsIgnoreCase("worker") && !vacantWorker.contains(child)&& !busyWorker.contains(child)&& !allWorkers.contains(child)) {
	        		//if it's new worker, and it's not deleted
	        		allWorkers.add(child);
	        		vacantWorker.add(child);
	        		zkClient.subscribeDataChanges("/prj/"+child, new DataListener(zkClient,jobsDone,vacantWorker,busyWorker));
	        	}
	        	
			}
			//update new job to masterPatj
			if(added != 0)zkClient.writeData("/prj/master", newJobs);
		}else {
			childNum = currentChilds.size();
			//if there's no master, the former process will be stopped
			if(!currentChilds.contains("master")) {
				vacantWorker.clear();
				busyWorker.clear();
				for(String child : currentChilds){
					if (child.length() > 5 && child.substring(0,6).equals("worker"))
						zkClient.delete("/prj/"+child);
				}
				stop.add("stop");
			}
			
		}
        
    }

}
