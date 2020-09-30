package zk;
import org.I0Itec.zkclient.ZkClient;
import java.io.ObjectOutputStream;
import java.io.ObjectInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ByteArrayInputStream;
import java.util.LinkedList;
import org.apache.zookeeper.*;
import java.io.IOException;
import org.apache.log4j.PropertyConfigurator;
import java.util.Queue;

public class Main {
	static ZkClient zkClient;
	static String masterPath = "/prj/master";
	static String workerPath = "/prj/worker";
	static int serverNumber ;
	static Queue<String> newJobs;
	static Queue<String> jobsDone;
	static Queue<String> vacantWorker;
	static Queue<String> busyWorker;
	static boolean isMaster = false;
	static Queue<String> stop ;
	
	public static void main(String[] args)  throws InterruptedException,IOException{
		
		//connect to zookeeper

		String zkAddress = "127.0.0.1" + ":2181";
		zkClient= new ZkClient(zkAddress,1000000);
		zkClient.setZkSerializer(new MySerializer());

		stop = new LinkedList<String>();
		try{
			zkClient.createPersistent("/prj");
		}catch(org.I0Itec.zkclient.exception.ZkNodeExistsException e ){}
		System.out.println("in");
		Process p = new Process(zkClient,stop);
		p.begin();
		while(true) {
			while(stop.peek() == null) {}
			if(stop.peek() == "stop") {
				stop.clear();
				zkClient.delete(workerPath);
				p = new Process(zkClient,stop);
				p.begin();
			}
		}
	}
	
	
	
	
}
