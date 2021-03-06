package zk;
import java.io.Serializable;

public class Job implements Serializable {
	public int jobNum;  //denote which job it belongs to(we can also get this info from the node this job from)
	public int paraNum;
	public int[] para;
	public int paraIndex;
	
	public Job(int jobNum, int[]  para) {
		this.jobNum = jobNum;
		this.para = para;
		this.paraNum = para.length;
	}
	
	public void setIndex(int n) {
		this.paraIndex = n;
	}
	
	public double calculate() {
		int points = para[paraIndex];
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
		return pi;
		
	}
}
