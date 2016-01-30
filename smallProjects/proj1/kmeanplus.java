import java.util.*;
import java.io.*;
import java.lang.*;

public class kmeanplus extends Cluster{
	public kmeanplus(String fileName, int k){
		super(fileName, k);
	}
	public int chooseCenter(double[] ws, double sum){
      double ran = Math.random() * sum;
      for(int i = 1; i < ws.length; i ++){
      	double x = Math.pow(ws[i], 2);
      	if(ran <= x)
      		return i;
      	else 
      	    ran -= x;      
      }
      return -1;
	}
	public void run(){
	  int n = list.size();
	  int c1 = (int)(Math.random() * n) + 1;
	  centers.put(c1, 1);
	  assignCenter(assign, c1);
	  double sum = 0.0;
	  double[] ws = new double[n + 1];
	  for(int i = 2; i <= k; i ++){
         for(int j = 1; j <= n; j ++){
         	ws[j] = Math.pow(calDistance(j, assign[j]), 2);
         	sum += ws[j];
         }
         int c = chooseCenter(ws, sum);
         assignCenter(assign, c);
         centers.put(c, i);
	  }
	  arrange();
	}
}