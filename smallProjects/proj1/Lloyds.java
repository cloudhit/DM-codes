import java.util.*;
import java.io.*;
import java.lang.*;

public class Lloyds extends Cluster{
	private double[][] cp;
  private int dim;
  private List<Integer>[] newsets;
	public Lloyds(String fileName, int kk){
	  super(fileName, kk);
	}
  public void noReceive(){
    centers.put(1,1);
    centers.put(2,2);
    centers.put(3,3);
    super.read();
    dim = list.get(0).size();
    cp = new double[k + 1][dim];
    for(Integer i : centers.keySet()){
      int s = centers.get(i);
      for(int j = 0; j < dim; j ++)
        cp[s][j] = list.get(i - 1).get(j);
    }
    assignNewCenters();
  }
  public void receiveTheOutput(Map<Integer, Integer> map, List<Integer>[] sets1, List<List<Double>> list1){
    list = list1;
    centers = map;
    newsets = sets1;
    dim = list.get(0).size();
    cp = new double[k + 1][dim];
    for(Integer i : centers.keySet()){
      int s = centers.get(i);
      //System.out.print(i + "&" + s + "*");
      for(int j = 0; j < dim; j ++)
        cp[s][j] = list.get(i - 1).get(j);
    }
  }
	public void findNewCenters(){
	  if(newsets == null)
	  	newsets = sets;
      for(int i = 0; i < newsets.length; i ++){
      double[] c = new double[dim];
      for(int k = 0; k < c.length; k ++) c[k] = 0.0;
      List<Integer> tmp = newsets[i];
      for(Integer j : tmp){
          List<Double> tlist = list.get(j - 1);
          for(int t = 0; t < tlist.size(); t ++)
            c[t] += tlist.get(t);
      }
      for(int k = 0; k < c.length; k ++) c[k] /= tmp.size();
      cp[i + 1] = c;
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
	  newsets = new ArrayList[k];
      for(int i = 1; i < 1005; i ++){
      	double min = Double.MAX_VALUE;
      	int index = 0;
        for(int j = 1; j <= k; j ++){
        	double t = calDistance(i, j);
          if(t < min){
          	min = t;
          	index = j; 
          }
        }
      if(newsets[index - 1] == null)
      	newsets[index - 1] = new ArrayList<Integer>();
      newsets[index - 1].add(i);
      }
	}
  public void run(){
    return;
  }
	public List<Integer>[] run1(){
    double pre = 0.0, cur = 0.0;
    int cnt = 0;
      while((cur = meanCost()) != pre){
        System.out.print(cur);
        pre = cur;
        cnt++;
      	findNewCenters();
      	assignNewCenters();
      }   
      return newsets;
	}
  public double meanCost(){
    double sum = 0;
    for(int i = 0; i < newsets.length; i ++){
     List<Integer> newset = newsets[i];
     for(Integer t : newset){
      sum += Math.pow(calDistance(t, i + 1), 2);
      }
    }
    System.out.println(sum);
    return sum;
   }
  public void display(){
    for(int i = 0; i < newsets.length; i ++){
      System.out.print("size:" + newsets[i].size() + " ");
    List<Integer> newset = newsets[i];
    for(Integer t : newset)
      System.out.print(t + ",");
    System.out.println();
    }
  }
	public void print(){
	  for(int i = 0; i < newsets.length; i ++){
	  	//System.out.print("size:" + newsets[i].size() + " ");
		List<Integer> newset = newsets[i];
		for(Integer t : newset)
			System.out.print(list.get(t - 1).get(0) + "," + list.get(t - 1).get(1) + ";");
		System.out.println();
	  }
	}
}