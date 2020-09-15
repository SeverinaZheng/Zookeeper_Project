package zk;
import org.apache.zookeeper.*;
import java.util.LinkedList;
import org.I0Itec.zkclient.ZkClient;
import java.util.Queue;

public class Process extends Node{
	private int serverNumber;
	private ZkClient zkClient;
	static String masterPath = "/prj/master";
	private String workerPath;
	Queue<String> newJobs;
	Queue<String> jobsDone;
	
	public Process(int num,ZkClient zkClient,Queue vacantWorker,Queue newJobs,Queue jobsDone) {
		this.serverNumber = num;
		this.zkClient = zkClient;
		this.workerPath = "/prj/worker" + serverNumber;
		this.vacantWorker = vacantWorker;
		this.newJobs = newJobs;
		this.jobsDone = jobsDone;
	}	
	public void run() {
		boolean isMaster = this.select();
		//capture when there's new job submitted and when the workers are done
		if(isMaster == true) {
			zkClient.subscribeChildChanges("/prj", new ChildListener(this,true,zkClient,newJobs,jobsDone));
			zkClient.subscribeDataChanges("/prj/master", new DataListener(this,true,zkClient,syncObj,jobsDone));
			
			//keep master alive
			//try{Thread.sleep(50000000);}catch(Exception e) {}
			while(true) {
				while(newJobs.peek() == null){
					System.out.println("wait for new job");
					try{Thread.sleep(5000);}catch(Exception e) {}
				}
				//System.out.println("New job!");
				System.out.println("jobs to do" + newJobs.toString());
				System.out.println("jobs done" + jobsDone.toString());
				System.out.println(newJobs.peek());
        		this.spreadOutJob(newJobs.poll().substring(3,4));
				
			}

		}else {
			//keep worker alive
			try{Thread.sleep(500000000);}catch(Exception e) {}
		}
	}
	
	public void spreadOutJob(String jobNum) {
		System.out.println(vacantWorker.toString());
		String jobPath = "/prj/job"+ jobNum;
		String para = zkClient.readData(jobPath);
		String[] paraArr= para.split(",");
		Queue<Integer> jobPara = new LinkedList<Integer>();
		for(String s : paraArr) {
			jobPara.add(Integer.parseInt(s));
		}
		int index = 0;
		String result = zkClient.readData("/prj/master");
		result +=jobNum+",";
		zkClient.writeData("/prj/master", result);
		if(paraArr.length <= vacantWorker.size()) {
			//if the arguments are less than current vacant worker
			while(vacantWorker.peek()!= null && jobPara.peek() != null) {
				Process worker = vacantWorker.poll();
				busyWorker.add(worker);
				Worker w = new Worker(worker,jobPara.poll(),syncObj,zkClient,vacantWorker,busyWorker,jobPath);
				w.start();
				index++;
			}
		}else {
			//if the arguments are more than current vacant worker
			while(index < paraArr.length) {
				while(vacantWorker.peek() == null) {
					System.out.println("wait 0.001s");
					try {
						Thread.sleep(1);
					}catch(InterruptedException e) {}
				}
				System.out.println("busy" + busyWorker.toString());
				System.out.println("vacant" + vacantWorker.toString());
				Process workerNow = vacantWorker.poll();
				busyWorker.add(workerNow);
				Worker w = new Worker(workerNow,jobPara.poll(),syncObj,zkClient,vacantWorker,busyWorker,jobPath);
				w.start();
				index++;
			}
		}
		
	}


	//the process creates /prj/master becomes master, others are workers
	public boolean select() {
		String result = "";
		try {
			zkClient.create(masterPath, result, CreateMode.PERSISTENT);
			System.out.println("master is " + serverNumber);
			return true;
		}catch(org.I0Itec.zkclient.exception.ZkNodeExistsException e ){
				zkClient.create(workerPath, serverNumber, CreateMode.PERSISTENT);
				System.out.println("new worker added : " + serverNumber);
				vacantWorker.add(this);
				return false;
		}
	}
	
	public String toString() {
		return this.workerPath;
		
	}

}
