import java.util.*;
import java.io.*;
import java.lang.*;

public abstract class Cluster{
   protected int k;
   protected int[] assign;
   protected readFile read_file;
   protected List<List<Double>> list;
   protected List<Integer>[] sets;
   protected Map<Integer, Integer> centers;
   protected int num;
   protected int dim;
   public void setZero(){
   	k = 0;
   	assign = null;
   	list = null;
   	sets = null;
   	centers = null;
   }
   public Cluster(String fileName, int kk){
   	read_file = new readFile(fileName);
   	k = kk;
    sets = new ArrayList[k];
    centers = new HashMap<Integer, Integer>();
    list = new ArrayList();
   }
   public void read(){
   	List<String> tmp = read_file.read();
   	num = tmp.size();
   	for(String str : tmp){
   		String[] strg = str.split("\\s");
   		List<Double> cur = new ArrayList();
   		for(int i = 1; i < strg.length; i ++){
           cur.add(Double.parseDouble(strg[i]));
   		}
   		dim = strg.length -1;
   		list.add(cur);
   	}
   	assign = new int[num + 1];
   }
   public double calDistance(int a, int b){
     List<Double> s = list.get(a - 1);
     List<Double> t = list.get(b - 1);
     double dis = 0.0;
     for(int i = 0; i < s.size(); i ++){
         dis += Math.pow(s.get(i) - t.get(i), 2);
     }
     return Math.sqrt(dis);
   }
   public void assignCenter(int[] assign, int c){
    for(int i = 1; i < assign.length; i ++){
       if(assign[i] == 0 || calDistance(assign[i], i) > calDistance(i, c))
       	assign[i] = c;
    }
   }
   public void arrange(){
   	int n = list.size();
   	for(int i = 1; i <= n; i ++){
   		int x = centers.get(assign[i]) - 1;
   		//System.out.println(x);
   	    if(sets[x] == null)
   	    	sets[x] = new ArrayList<Integer>();
   		sets[x].add(i);
   	}
   }
   public abstract void run();
   public void printAxis(){
    for(Integer i : centers.keySet()){
      //System.out.print("center : " + i + " ");
      List<Integer> set = sets[centers.get(i) - 1];
      //System.out.print("size:" + set.size() + " ");
      for(Integer t : set)
        System.out.print(list.get(t - 1).get(0) + "," + list.get(t - 1).get(1) + ";");
      System.out.println();
    }
   }
   public void display(){
   	for(Integer i : centers.keySet()){
   		System.out.print("center : " + i + " ");
   		List<Integer> set = sets[centers.get(i) - 1];
   		System.out.print("size:" + set.size() + " ");
   		for(Integer t : set)
   			System.out.print(t + ",");
   		System.out.println();
   	}
   }
   public double centerCost(){
     double cost = 0;
     for(int i = 1; i <= list.size(); i ++)
     	cost = Math.max(cost, calDistance(i, assign[i]));
     return cost;
   }
   public double meanCost(){
     double cost = 0;
     for(int i = 1; i <= list.size(); i ++)
     	cost += Math.pow(calDistance(i, assign[i]), 2);
     return cost;
   }
   public Map<Integer, Integer> getCenters(){
   	return centers;
   }
   public List<Integer>[] getSets(){
   	return sets;
   }
   public List<List<Double>> getList(){
   	return list;
   }

}