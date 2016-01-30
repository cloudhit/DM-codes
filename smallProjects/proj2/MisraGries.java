import java.util.*;
import java.lang.*;
import java.io.*;

public class MisraGries{
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
	public static HashMap run(){
		int k = 10;
		HashMap<Character, Integer> map = new HashMap<Character, Integer>();
		String str = readString();
        for(int i = 0; i < str.length(); i += 2){
        	Set<Character> set = new HashSet();
        	char c = str.charAt(i);
            if(map.containsKey(c))
            	map.put(c, map.get(c) + 1);
            else
            	if(map.size() < k - 1)
            		map.put(c, 1);
            	else{
            		for(Map.Entry<Character, Integer> entry : map.entrySet()){
            			if(entry.getValue() == 1)
            				set.add(entry.getKey());
            			 map.put(entry.getKey(), entry.getValue() - 1);
            		}
            	}
            for(Character tmp : set)
            	map.remove(tmp);
        }
        return map;
	}
	public static void main(String[] args) {
		HashMap<Character, Integer> map = run();
		for(Map.Entry<Character, Integer> entry : map.entrySet()){
			System.out.println(entry.getKey() + " " + entry.getValue());
		}
	}
}