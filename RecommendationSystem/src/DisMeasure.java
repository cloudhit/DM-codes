import java.util.*;
import java.io.*;
import java.lang.*;
public class DisMeasure{
    private double[] d1 = new double[statistics.dimNum + 1];
    private double[] d2 = new double[statistics.dimNum + 1];
	public DisMeasure(double[] t1, double[] t2){
		this.d1 = t1;
		this.d2 = t2;
	}
	public double EuDistance(){
		double ans = 0.0;
		for(int i = 1; i <= statistics.dimNum; i ++){
			ans += Math.pow(d1[i] - d2[i],2);
		}
		return Math.sqrt(ans);
	}

}