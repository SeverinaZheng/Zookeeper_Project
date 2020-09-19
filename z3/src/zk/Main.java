package zk;
import org.I0Itec.zkclient.ZkClient;
import java.util.LinkedList;
import org.apache.zookeeper.*;
import java.io.IOException;
import org.apache.log4j.PropertyConfigurator;
import java.util.Queue;

public class Main {
	static ZkClient zkClient;
	static String masterPath = "/prj/master";
	static String workerPath = "/prj/worker3";
	static int serverNumber = 3;
	static Queue<String> newJobs;
	static Queue<String> jobsDone;
	static Queue<String> vacantWorker;
	static Queue<String> busyWorker;
	static boolean isMaster = false;
	
	public static void main(String[] args)  throws InterruptedException,IOException{
		
		//connect to zookeeper
		vacantWorker = new LinkedList<String>();
		busyWorker = new LinkedList<String>();
		newJobs = new LinkedList<String>();
		jobsDone = new LinkedList<String>();
		zkClient= new ZkClient("127.0.0.1:2181",1000000);
		try{
			zkClient.createPersistent("/prj");
		}catch(org.I0Itec.zkclient.exception.ZkNodeExistsException e ){}
		System.out.println("in");
		isMaster = select();
		if(isMaster) {
			zkClient.subscribeChildChanges("/prj", new ChildListener(newJobs,jobsDone,vacantWorker,busyWorker,zkClient));
			while(true) {
				while(newJobs.peek() == null){
					System.out.println("wait for new job");
					try{Thread.sleep(5000);}catch(Exception e) {}
				}
				//System.out.println("New job!");
				System.out.println("jobs to do" + newJobs.toString());
				System.out.println("jobs done" + jobsDone.toString());
				System.out.println(newJobs.peek());
        		spreadOutJob(newJobs.poll().substring(3,4));
				
			}
		}else {
			Queue<String> para = new LinkedList();
			zkClient.subscribeDataChanges(workerPath, new DataListener(para,zkClient,jobsDone));
			//keep worker alive
			while(true) {
				while(para.peek() == null) {
					System.out.println("wait for job");
					try {
						Thread.sleep(5000);
					}catch(InterruptedException e) {}
				}
				calculate(para.poll());
			}
			
			
		}
	   
	}
	
	
	public static boolean select() {
		String result = "";
		try {
			zkClient.create(masterPath, result, CreateMode.PERSISTENT);
			System.out.println("master is " + serverNumber);
			return true;
		}catch(org.I0Itec.zkclient.exception.ZkNodeExistsException e ){
				zkClient.create(workerPath, "", CreateMode.PERSISTENT);
				System.out.println("new worker added : " + serverNumber);
				return false;
		}
	}
	
	
	public static void spreadOutJob(String jobNum) {
		String jobPath = "/prj/job"+ jobNum;
		String para = zkClient.readData(jobPath);
		String[] paraArr= para.split(",");
		int index = 0;
		String result = zkClient.readData("/prj/master");
		result +=jobNum+",";
		zkClient.writeData("/prj/master", result);
		if(paraArr.length <= vacantWorker.size()) {
			//if the arguments are less than current vacant worker
			while(vacantWorker.peek()!= null && index < paraArr.length) {
				String worker = vacantWorker.poll();
				busyWorker.add(worker);
				String jobAndPara = "job" + jobNum + "," + paraArr[index];
				zkClient.writeData("/prj/"+worker, jobAndPara);
				index++;
			}
		}else {
			//if the arguments are more than current vacant worker
			while(index < paraArr.length) {
				while(vacantWorker.peek() == null) {
					System.out.println("wait 5s");
					try {
						Thread.sleep(5000);
					}catch(InterruptedException e) {}
				}

				System.out.println("vacant" + vacantWorker.toString());
				String workerNow = vacantWorker.poll();
				busyWorker.add(workerNow);
				zkClient.writeData("/prj/"+workerNow,  "job" + jobNum + "," + paraArr[index]);
				index++;
				System.out.println("busy" + busyWorker.toString());
			}
		}
	}
	
	
	public static void calculate(String para) {
		String[] jobAndPara = para.split(",");
		String resultPath = "/prj/"+jobAndPara[0]+"-result";
		int points = Integer.parseInt(jobAndPara[1]);
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
		String result = zkClient.readData(resultPath);
		result +=pi+",";
		zkClient.writeData(resultPath, result);
		zkClient.writeData(workerPath, "");
		System.out.println(serverNumber + "calculate" + pi+ "for "+ jobAndPara[0]);
	}
}
