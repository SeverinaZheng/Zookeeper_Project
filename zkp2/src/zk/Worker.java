package zk;
import org.apache.zookeeper.*;
import java.util.LinkedList;
import org.I0Itec.zkclient.ZkClient;
import java.util.Queue;


public class Worker extends Thread{
	private Process p;
	private int points;
	Object syncObj;
	private ZkClient zkClient;
	Queue<Process> vacantWorker ;
	Queue<Process> busyWorker;
	String resultPath;
	
	public Worker(Process p, int points, Object obj, ZkClient zkClient,Queue<Process> vacant,Queue<Process> busy,String jobPath) {
		this.p = p;
		this.points = points;
		this.syncObj = obj;
		this.zkClient = zkClient;
		this.vacantWorker = vacant;
		this.busyWorker = busy;
		this.resultPath = jobPath + "-result";
	}
	
	public void run() {
		System.out.println(p.toString().substring(5) + " starts " + resultPath.substring(5,9));
		int inCircle = 0;
		double x, y;
		double pi;
		for(int i = 0; i < points; i++) {
			x = Math.random();
			y = Math.random();
			if(x * x + y * y <= 1) 
				inCircle++;
			//System.out.println(x +";" + "y"+";"+inCircle);
		}
		pi = (double)4*inCircle/points;
		//System.out.println(this.toString() + " cal result is "+ pi+"with parameter" +points);
		synchronized(syncObj) {
			//System.out.println("Enter sync");
			String result = zkClient.readData(resultPath);
			result +=pi+",";
			zkClient.writeData(resultPath, result);
			//System.out.println(result);
			
		}
		//System.out.println(this.p.toString() + "uploaded");
		busyWorker.remove(p);
		vacantWorker.add(p);
	}
	
}
