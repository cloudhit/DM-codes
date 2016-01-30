import java.util.*;
import java.io.*;
import java.lang.*;

public class test{
	public static void main(String[] args) {
		HierarchicalClustering hcc = new HierarchicalClustering("C1.txt");
		hcc.read();
		//hcc.getCluster(methodsort.singleLink);
		
		
		//hcc.getCluster(methodsort.completeLink);
		
		
		hcc.getCluster(methodsort.meanLink);
		//hcc.printAxis();
		hcc.display();
		
		/*Gonzalez gz = new Gonzalez("C2.txt", 3);
		gz.read();
		gz.run();
		gz.display();
		gz.printAxis();
		System.out.print(gz.centerCost() + " " + gz.meanCost());*/
		/*
		double[] costs = new double[51];
		kmeanplus km = null;
		int diff = 0;
		for(int k = 1; k <= 50; k ++){
	    km = new kmeanplus("C2.txt", 3);
		km.read();
		km.run();
		//costs[k] = km.meanCost();
		//System.out.println("\\hline " + k + "&" + costs[k] + "\\\\");
	    
	    
		//km.display();
	
		/*
		Lloyds ly = new Lloyds("C2.txt", 3);
		List<Integer>[] before = km.getSets();
		ly.receiveTheOutput(km.getCenters(), km.getSets(), km.getList());
        List<Integer>[] after = ly.run1();
        //ly.display();
        //ly.print();
        costs[k] = ly.meanCost();
        boolean flag = true;
        for(int m = 0; m < after.length; m ++){
        	List<Integer> tmp = after[m];
        	Set<Integer> some = new HashSet();
        	for(int s = 0; s < tmp.size(); s++)
        		some.add(tmp.get(s));
        	boolean found = false;
        	for(int t = 0; t < before.length; t ++)
              if(before[t].size() == after[m].size()){
                 List<Integer> tmp1 = before[t];
                 for(int ss = 0; ss < tmp1.size(); ss++)
                 	if(!some.contains(tmp1.get(ss)))
                     {
                       flag = false;
                       break;
                     }
                found = true;
              	break;
              }
             if(!found) flag = false;
              if(!flag){
              	diff ++;
              	break;
              }
        	
        }
        //System.out.println(ly.meanCost());
        }
        Arrays.sort(costs);
	    for(int k = 1; k <= 50; k ++)
	    	System.out.print(costs[k] + ";");
	    System.out.println();
	    System.out.println(diff * 1.0 / 50);*/
		/*
        Lloyds ly1 = new Lloyds("C2.txt",3);
        ly1.noReceive();
        ly1.run();
        ly1.display();
        ly1.print();
        System.out.println(ly1.meanCost());*/
        /*double sum = 0;
        for(int i = 1; i <= 1; i ++){
		kmedian kmed = new kmedian("C3.txt", 5);
		kmed.run();
		sum += kmed.cost();
		kmed.displayCenters();
	    }
		sum /= 100;
		//System.out.print(sum);
		*/
	}
}