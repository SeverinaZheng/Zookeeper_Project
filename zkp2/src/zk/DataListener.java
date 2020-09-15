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
	private Process pmaster;
	ZkClient zkClient;
	private Client client;
    Object syncObj;
	Queue<String> jobsDone;
	
	public DataListener(Object p, boolean isMaster,ZkClient zkClient,Object syn,Queue<String> jobsDone) {
		if(isMaster) this.pmaster = (Process)p;
		else this.client = (Client) p;
		this.zkClient = zkClient;
		this.syncObj = syn;
		this.jobsDone = jobsDone;
	}
	
	public void handleDataChange(String parentPath, Object o) throws Exception {
		if(parentPath.substring(parentPath.length()-6).equalsIgnoreCase("result")){
			String data = (String) o;
	        String[] dataArr= data.split(",");
	        String para = zkClient.readData(parentPath.substring(0,9));
	        String[] paraArr = para.split(",");
	        if(dataArr.length == paraArr.length){
		        	int resultNum = dataArr.length;
		        	double resultSum = 0;
		        	for(int i = 0 ; i < dataArr.length;i++) {
		        		resultSum += Double.parseDouble(dataArr[i]);
		        		//System.out.println("Sum is " +resultSum+"Num is "+resultNum);
		        	}
		        	double result = (double)resultSum/resultNum;
					jobsDone.add(parentPath.substring(5, 9));
					client.announceResult(result,parentPath.substring(5,9));
					zkClient.unsubscribeDataChanges(parentPath, this);
					zkClient.deleteRecursive(parentPath);
					zkClient.deleteRecursive(parentPath.substring(0, 9));
	        }
		}
			
			zkClient.writeData("/prj/master", "");
			//double death = Math.random();
			//if(death > 0.5) {
			//	System.out.println("master is dead");
			//}
		
        
    }
	
	public void handleDataDeleted(String parentPath) throws Exception {}

}
