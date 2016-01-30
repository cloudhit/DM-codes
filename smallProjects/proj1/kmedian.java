import java.util.*;
import java.lang.*;
import java.io.*;

public class kmedian extends Cluster{
   private double[][] cp;
   private double r;
   public kmedian(String fileName, int kk){
     super(fileName, kk);
     read();
     cp = new double[k + 1][dim];
     r = 0.008;
   }
   public void chooseInitials(){
   	 int x = -1;
   	 Set<Integer> set = new HashSet<Integer>();
   	 for(int i = 1; i <= k; i ++){
   	   while(x == -1 || set.contains(x))
   	 	x = (int)(Math.random() * num)+ 1;
   	   set.add(x);
   	   //System.out.println(x);
   	 }
   	 int cur = 1;
   	 for(Integer i : set){
   	 	List<Double> tmp = list.get(i - 1);
        for(int j = 0; j < dim; j ++)
        	cp[cur][j] = tmp.get(j);
        cur ++;
   	 }
   }
	public double calDistance(int a, int b){
       List<Double> tmp = list.get(a - 1);
       double sum = 0;
       for(int i = 0; i < tmp.size(); i ++){
       	sum += Math.pow(tmp.get(i) - cp[b][i], 2);
       }
       return Math.sqrt(sum);
	}
	public void assignNewCenters(){
	  sets = new ArrayList[k];
      for(int i = 1; i <= 1000; i ++){
      	double min = Double.MAX_VALUE;
      	int index = 0;
        for(int j = 1; j <= k; j ++){
          double t = calDistance(i, j);
          if(t < min){
          	min = t;
          	index = j; 
          }
        }
      //System.out.println(i + " " + index);
      if(sets[index - 1] == null)
      	sets[index - 1] = new ArrayList<Integer>();
      sets[index - 1].add(i);
      }
   }
   public void run(){
       chooseInitials();
       assignNewCenters();
       double pre = 0.0, cur;
       int cnt = 0;

       while((cur = cost()) != pre){
        pre = cur;
       	cnt++;
       for(int i = 0; i < k; i ++){
       	 List<Integer> tmp = sets[i];
       	 //System.out.println(tmp == null);
       	 for(int j = 0; j < dim; j ++){
       	 	double denominator = 0.0, numerator = 0.0, res = 0.0;
       	 	for(Integer k : tmp){
       	 		numerator = cp[i + 1][j] - list.get(k - 1).get(j);
       	 		denominator = calDistance(k, i + 1);
       	 		if(denominator != 0)
                 res += numerator / denominator;
       	 	}
            cp[i + 1][j] -= r * res;

         }
        }
        assignNewCenters();
       }
       System.out.println(cnt);
   }
   public void print(){
   	for(int i = 0; i < sets.length; i ++){
	  	//System.out.print("size:" + sets[i].size() + " ");
		List<Integer> set = sets[i];
		for(Integer t : set){
			for(int k = 0; k < dim; k ++)
			System.out.print(list.get(t - 1).get(k) + ",");
		System.out.print(";");
	}
		System.out.println();
	  }

   }
   public double cost(){
   	    double sum = 0;
   	   	for(int i = 0; i < sets.length; i ++){
	  	//System.out.print("size:" + sets[i].size() + " ");
		List<Integer> set = sets[i];
		for(Integer t : set){
			sum += calDistance(t, i + 1);
	    }
		
	  }
	  System.out.println(sum);
	  return sum;
   }
   public void display(){
	  for(int i = 0; i < sets.length; i ++){
	  	System.out.print("size:" + sets[i].size() + " ");
		List<Integer> set = sets[i];
		for(Integer t : set)
			System.out.print(t + " ");
		System.out.println();
	  }
   }
   public void displayCenters(){
    for(int i = 1; i <= k; i ++){
      System.out.print(i + " ");
      for(int j = 0; j < dim; j ++)
        if(j != dim)
         System.out.print(cp[i][j] + " ");
        else
         System.out.print(cp[i][j]);
      System.out.println();
    }
   }

}