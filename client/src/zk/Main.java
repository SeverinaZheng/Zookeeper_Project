package zk;
import org.I0Itec.zkclient.ZkClient;
import java.util.LinkedList;
import org.apache.zookeeper.*;
import java.io.IOException;
import org.apache.log4j.PropertyConfigurator;
import java.util.Queue;

public class Main {
	static ZkClient zkClient;
	static Queue<String> jobAndResult;
	
	public static void main(String[] args)  throws InterruptedException,IOException{
		//connect to zookeeper
		jobAndResult = new LinkedList<String>();
		String zkAddress = args[0] + ":2181";
		zkClient= new ZkClient(zkAddress,1000000);
		zkClient.setZkSerializer(new MySerializer());
		System.out.println("in");
		Client c = new Client(zkClient);
		c.runClient();
		try {Thread.sleep(100000000);}catch(Exception e) {}
	}
	
}