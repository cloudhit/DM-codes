import java.util.*;
import java.io.*;
import java.lang.*;

public class HierarchicalClustering{
	private Map<Integer, List<Integer>> clusters;
	private Map<pair, Double> distances;
	private List<String> list;
	private Double[][] v;
	private readFile read_file;
	public HierarchicalClustering(String FileName){
		clusters = new HashMap<Integer, List<Integer>>();
		distances = new HashMap<pair, Double>();
		read_file = new readFile(FileName);
	}
	public void read(){
		list = read_file.read();
		int n = list.get(0).split("\\s").length;
		v = new Double[list.size()][n - 1];
		for(int i = 0; i < list.size(); i ++){
			List<Integer> set = new ArrayList<Integer>();
			set.add(i);
            String[] str = list.get(i).split("\\s");
            for(int j = 1; j < str.length; j ++)
            	v[i][j - 1] = Double.parseDouble(str[j]); 
			clusters.put(i, set);
		}
	}
	public void initialDistances(){
		for(int i = 0; i < clusters.size(); i ++)
			for(int j = i + 1; j < clusters.size(); j ++){
              int a = clusters.get(i).get(0);
              int b = clusters.get(j).get(0);
              double sum = 0.0;
              for(int k = 0; k < v[a].length; k ++)
              	sum += Math.pow(v[a][k] - v[b][k], 2);
              sum = Math.sqrt(sum);
              distances.put(new pair(a, b), sum);
			}
			Iterator iter = distances.entrySet().iterator();
			while(iter.hasNext()){
				Map.Entry entry = (Map.Entry)iter.next();
				pair p = (pair)entry.getKey();
				double d = (double)entry.getValue();
				//System.out.println(p.getKey() + " " + p.getValue() + " " + d);
			}
	}
	public pair findMin(){
		Iterator iter = distances.entrySet().iterator();
		pair p = null;
		//System.out.println(distances.size());
		double min = Double.MAX_VALUE;
		while(iter.hasNext()){
		   Map.Entry entry = (Map.Entry)iter.next();
           double d = (double)entry.getValue();
           if(d < min){
           	 min = d;
           	 p = (pair)entry.getKey();
           }
		}
		return p;
	}
	public double methods(int a, int b, int c, methodsort sort){
		double dis = 0.0;
        switch(sort){
        	case singleLink:
        	{ 
        		double t1 = distances.get(new pair(a, c));
                double t2 = distances.get(new pair(b, c));
                dis = Math.min(t1, t2);
        		break;
        	}
        	case completeLink:
        	{        		
        		double t1 = distances.get(new pair(a, c));
                double t2 = distances.get(new pair(b, c));
                dis = Math.max(t1, t2);
        		break;
        	}
        	case meanLink:
        	{
                if(v[b][0] != Double.MIN_VALUE)
                {
                   for(int i = 0; i < v[b].length; i ++){
                   	 v[a][i] = v[a][i] * clusters.get(a).size() + v[b][i] * clusters.get(b).size();
                   	 v[a][i] /= clusters.get(a).size() + clusters.get(b).size();
                   }
                   v[b][0] = Double.MIN_VALUE;
                }
                for(int k = 0; k < v[b].length; k ++){
                	dis += Math.pow(v[a][k] - v[c][k], 2);
                }
                dis = Math.sqrt(dis);
        		break;
        	}
        }
        return dis;
	}
	public void mergeSet(int a, int b, methodsort sort){
		List<Integer> la = clusters.get(a);
		List<Integer> lb = clusters.get(b);
		distances.remove(new pair(a, b));
		Map<pair, Double> newdistances = new HashMap<pair, Double>();
	    for(pair p : distances.keySet()){
            if(p.getKey() == a || p.getValue() == a){
				int c = (p.getKey() == a)? p.getValue(): p.getKey();
                double newdis = methods(a, b, c, sort);
                newdistances.put(new pair(a, c), newdis);
			}else if(p.getKey() == b || p.getValue() == b){
				int c = (p.getKey() == b)? p.getValue(): p.getKey();
                double newdis = methods(a, b, c, sort);                
                newdistances.put(new pair(a, c), newdis);
			}else
			    newdistances.put(p, distances.get(p));
	    }
	    distances = newdistances;
	    for(Integer tmp : lb)
			la.add(tmp);
		clusters.remove(b);
	}
	public void getCluster(methodsort sort){
       int size = clusters.size();
       initialDistances();
       while(size > 4){
          pair p = findMin();
          mergeSet(p.getKey(), p.getValue(), sort);
          size = clusters.size();
       }
	}
	public void printAxis(){
		Iterator iter = clusters.entrySet().iterator();
		while(iter.hasNext()){
			Map.Entry entry = (Map.Entry)iter.next();
			List<Integer> cluster = (List<Integer>)entry.getValue();
			for(Integer i : cluster)
				System.out.print(v[i][0] + "," + v[i][1] + ";");
			System.out.println();
		}
	}
	public void display(){
		Iterator iter = clusters.entrySet().iterator();
		while(iter.hasNext()){
			Map.Entry entry = (Map.Entry)iter.next();
			List<Integer> cluster = (List<Integer>)entry.getValue();
			for(Integer i : cluster)
				System.out.print(i + " ");
			System.out.println();
		}
	}
}
