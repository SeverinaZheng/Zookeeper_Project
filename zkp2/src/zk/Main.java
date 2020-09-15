package zk;
import org.I0Itec.zkclient.ZkClient;
import java.util.LinkedList;
import org.apache.zookeeper.*;
import java.io.IOException;
import org.apache.log4j.PropertyConfigurator;
import java.util.Queue;

public class Main {
	static ZkClient zkClient;
	public static void main(String[] args)  throws InterruptedException,IOException{
		int numThreads = 6;
		
		//connect to zookeeper
		Queue<Process> vacantWorker = new LinkedList<Process>();
		Queue<String> newjobs = new LinkedList<String>();
		Queue<String> jobsDone = new LinkedList<String>();
		zkClient= new ZkClient("127.0.0.1:2181",1000000);
		zkClient.createPersistent("/prj");
		System.out.println("in");
		//create numThreads processes, can be either worker or master
	    for(int i = 0; i < numThreads ; i++) {
			Process obj = new Process(i,zkClient,vacantWorker,newjobs,jobsDone);
			obj.start();
		}
	    Client c = new Client();
	    c.runClient();
	   
	}
}
