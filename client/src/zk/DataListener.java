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
	Client c;
	
	
	public DataListener(Client c,ZkClient zkClient) {
		this.zkClient = zkClient;
		this.c = c;
	}
	public void handleDataChange(String parentPath, Object o) throws Exception {
		//since all worker return result to /prj/jobx-result node 
		//check if the # of result in /prj/jobx-result node == # of parameters in /prj/jobx
		//if so, then all work are done and we can return to client
		if(parentPath.substring(parentPath.length()-6).equalsIgnoreCase("result")){
			String data = (String) o;
	        String[] dataArr= data.split(",");
	        Job job = (Job)zkClient.readData(parentPath.substring(0,9));
	        int[] paraArr = job.para;
	        if(dataArr.length == paraArr.length){
		        	int resultNum = dataArr.length;
		        	double resultSum = 0;
		        	for(int i = 0 ; i < dataArr.length;i++) {
		        		resultSum += Double.parseDouble(dataArr[i]);
		        		//System.out.println("Sum is " +resultSum+"Num is "+resultNum);
		        	}
		        	double result = (double)resultSum/resultNum;
					c.announceResult(result, parentPath.substring(5,9));
					zkClient.unsubscribeDataChanges(parentPath, this);
					zkClient.deleteRecursive(parentPath);
					zkClient.deleteRecursive(parentPath.substring(0, 9));
	        }
		}else if(parentPath.substring(5,11).equalsIgnoreCase("worker")) {
			String jobAndPara = zkClient.readData(parentPath);
			para.add(jobAndPara);
		}
			
			//double death = Math.random();
			//if(death > 0.5) {
			//	System.out.println("master is dead");
			//}
		
        
    }
	
	public void handleDataDeleted(String parentPath) throws Exception {}

}
