package zk;
import java.util.Queue;
import java.util.LinkedList;

public class Node extends Thread {
	static Queue<Process> vacantWorker = new LinkedList<Process>();
	static Queue<Process> busyWorker = new LinkedList<Process>();
	static Queue<Queue<Integer>> jobs = null;
	static Object syncObj = new Object();
}
