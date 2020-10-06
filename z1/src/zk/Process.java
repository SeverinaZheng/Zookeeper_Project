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

public class Process {
	static ZkClient zkClient;
	static ZkClient zkClient2;
	static String masterPath = "/prj/master";
	static String workerPath = "/prj/worker";
	static int serverNumber;
	static Queue<String> newJobs;
	static Queue<String> jobsDone;
	static Queue<String> vacantWorker;
	static Queue<String> busyWorker;
	static boolean isMaster = false;
	static Queue<String> stop ;
	
	public Process(ZkClient zkClient,ZkClient zkClient2, Queue<String> stop) {
		this.zkClient = zkClient;
		this.zkClient2 = zkClient2;
		this.stop = stop;
	}
	public void begin() {
		vacantWorker = new LinkedList<String>();
		busyWorker = new LinkedList<String>();
		newJobs = new LinkedList<String>();
		jobsDone = new LinkedList<String>();
		
		
		isMaster = select();
		if(isMaster) {
			Queue<String> died = new LinkedList<String>();
			serverNumber = 0;
			//setup 2 zk connections(only one listener is activated per zkClient)
			//we will store job under masterPath;
			zkClient2.subscribeDataChanges(masterPath, new DataListener(zkClient2,this,died));
			zkClient.subscribeChildChanges("/prj", new ChildListener(this,newJobs,jobsDone,vacantWorker,busyWorker,zkClient));
			stop.add("master");
			//a node how many workers have been established, it's mechanism give out unrepeated serverNum
			if(!zkClient.getChildren("/prj").contains("wkNum"))
				zkClient.createPersistent("/prj/wkNum", "0");
			//if a master is selected after the former master died, it pick up all
			//jobs remaining to start work
			for(String s: zkClient.getChildren("/prj")) {
				if(s.length() < 7 && s.substring(0,3).equals("job") && !newJobs.contains(s))
					newJobs.add(s);
			}
			if(newJobs != null) {
				zkClient.writeData(masterPath, newJobs);
			}
			
			while(died.isEmpty()) {}
			stop.add("masterdie");
			
		}else {
			Queue<String> para = new LinkedList();
			//we will store parameter and logic into a Job object, write to workerPath
			//where index param in this object denotes which parameter this worker node will calculate
			zkClient.subscribeDataChanges(workerPath, new DataListener(serverNumber,para,zkClient,jobsDone));
			zkClient.subscribeChildChanges("/prj", new ChildListener(this,stop,newJobs,jobsDone,vacantWorker,busyWorker,zkClient));
			//if stop queue contains "stop", it means the master died
			while(stop.peek() == null) {System.out.print("");}
			System.out.println("reselect");
		}
	}
	
	//try to create /prj/master node, which will denote the only master(return true if is master)
		//if failed, then create a worker node 
		public static boolean select() {
			String result = "";
			try {
				zkClient.create(masterPath, result, CreateMode.PERSISTENT);
				System.out.println("master is " + serverNumber);
				return true;
			}catch(org.I0Itec.zkclient.exception.ZkNodeExistsException e ){
				while(!zkClient.getChildren("/prj").contains("wkNum")) {
					try{Thread.sleep(100);}catch(Exception ee) {}
				}
				serverNumber = Integer.parseInt(zkClient.readData("/prj/wkNum"))+1;
				zkClient.writeData("/prj/wkNum", Integer.toString(serverNumber));
				if(workerPath.length() != 11)
					workerPath = workerPath.substring(0,11) + serverNumber;
				else
					workerPath = workerPath + serverNumber;
				zkClient.create(workerPath, "", CreateMode.PERSISTENT);
				System.out.println("new worker added : " + serverNumber);
				return false;
			}
		}
		
		//when a /prj/job node is newly created,
		//it will trigger spreadOutJob method to read in all parameter
		//and write to available worker nodes with jobK+parameter
		public static void spreadOutJob(String jobNum) {
			String jobPath = "/prj/job"+ jobNum;
			Job job = zkClient.readData(jobPath);
			int[] paraArr = job.para;
			int index = 0;
			if(paraArr.length <= vacantWorker.size()) {
				//if the arguments are less than current vacant worker
				while(vacantWorker.peek()!= null && index < paraArr.length) {
					String worker = vacantWorker.poll();
					busyWorker.add(worker);
					job.setIndex(index);
					zkClient.writeData("/prj/"+worker, job);
					index++;
				}
			}else {
				//if the arguments are more than current vacant worker
				while(index < paraArr.length) {
					while(vacantWorker.peek() == null) {
						try {
							Thread.sleep(1000);
						}catch(InterruptedException e) {}
					}
				//if there's vacant worker, we write the parameter to /prj/worker node
					System.out.println("vacant" + vacantWorker.toString());
					String workerNow = vacantWorker.poll();
					busyWorker.add(workerNow);
					job.setIndex(index);
					zkClient.writeData("/prj/"+workerNow, job);
					index++;
					System.out.println("busy" + busyWorker.toString());
				}
			}
		}
		
		
		

}
