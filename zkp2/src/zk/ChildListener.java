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
	private Process pmaster;
	private Client client;
	ZkClient zkClient;
	Queue<String> newJobs;
	Queue<String> jobsDone;
	
	public ChildListener(Object p, boolean isMaster,ZkClient zkClient,Queue newJobs,Queue jobsDone) {
		if(isMaster) this.pmaster =(Process) p;
		else this.client =(Client)p;
		this.zkClient = zkClient;
		this.newJobs = newJobs;
		this.jobsDone = jobsDone;
	}
	
	public void handleChildChange(String parentPath, List<String> currentChilds) throws Exception {
		for(String child : currentChilds) {
        	if(child.length() < 6 && child.substring(0,3).equalsIgnoreCase("job")) {
        		if(!newJobs.contains(child) && !jobsDone.contains(child) ) {
        			newJobs.add(child);
        		}
        	}		
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
