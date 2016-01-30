import java.util.*;
import java.lang.*;
import java.io.*;

public class CountMin{
	public static String readString(){
		FileInputStream file = null;
		InputStreamReader isr = null;
		BufferedReader br = null;
		String str = null;
		try{
			file = new FileInputStream("S2.txt");
			isr = new InputStreamReader(file);
			br = new BufferedReader(isr);
			str = br.readLine();
		}catch (FileNotFoundException e) {
         System.out.println("can't find file");
        }catch (IOException e) {
         System.out.println("read/write fails");
        }finally {
        try {
           br.close();
           isr.close();
           file.close();
        }catch (IOException e) {
           e.printStackTrace();
       }
       return str;
     }
	}
	public static int HashFunction(int sort, int key){
        return sort * key % 29 % 10;
	}
	public static int[][] run(){
       String str = readString();
       int k = 10;
       int t = 5;
       int[][] C = new int[t + 1][k];
       for(int i = 0; i < str.length(); i += 2){
       	int a = str.charAt(i) - 'a' + 1;
        for(int j = 1; j <= t; j ++)
        	C[j][HashFunction(j,a)] ++;
       }
       return C;
	}
	public static void main(String[] args) {
	   int[][] C = run();
	   for(int x = 1; x <= 3; x ++){
	   	int min = Integer.MAX_VALUE;
	   	for(int i = 1; i <= 5; i ++)
	   		min = Math.min(min, C[i][HashFunction(i,x)]);
	   	System.out.println(min);
	   }
	}
}