import java.util.*;
import java.io.*;
import java.lang.*;

public class Gonzalez extends Cluster{

   public Gonzalez(String fileName, int kk){
   	super(fileName, kk);
   }
   public void run(){
   	int n = list.size();
   	int c1 = (int)(Math.random() * n) + 1;
   	centers.put(c1, 1);
   	assignCenter(assign, c1);
   	for(int i = 2; i <= k; i ++){
   		double M = 0.0, r = 0.0;
   		int c = 1;
   		for(int j = 1; j <= n; j ++)
   			if((r = calDistance(j, assign[j])) > M){
   				c = j;
   				M = r;
   			}
   		assignCenter(assign, c);
   		centers.put(c, i);
   	}
   	arrange();
   }
}