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
	static ZkClient zkClient2;
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

		String zkAddress = args[0] + ":2181";
		zkClient= new ZkClient(zkAddress,1000000);
		zkClient2= new ZkClient(zkAddress,1000000);
		zkClient.setZkSerializer(new MySerializer());
		zkClient2.setZkSerializer(new MySerializer());
		
		stop = new LinkedList<String>();
		try{
			zkClient.createPersistent("/prj");
		}catch(org.I0Itec.zkclient.exception.ZkNodeExistsException e ){}
		System.out.println("in");
		Process p = new Process(zkClient,zkClient2,stop);
		p.begin();
		while(true) {
			while(stop.peek() == null) {}
			//if it was a worker, we store "stop" in list stop
			//if it was master, stop's top element is "masterdie"
			//so only worker will restart the select procedure
			//master will die out directly
			if(stop.peek() == "stop") {
				stop.clear();
				zkClient.delete(workerPath);
				p = new Process(zkClient,zkClient2,stop);
				p.begin();
			}else
				break;
		}
	}
	
	
	
	
}
