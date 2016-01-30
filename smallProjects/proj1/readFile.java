import java.util.*;
import java.io.*;
import java.lang.*;

public class readFile{
	private String FileName;
	private List<String> list;
	public readFile(String file_name){
		FileName = file_name;
		list = new ArrayList<String>();
	}
	public List<String> read(){
		FileInputStream file = null;
		InputStreamReader isr = null;
		BufferedReader br = null;
		try{
			file = new FileInputStream(FileName);
			isr = new InputStreamReader(file);
			br = new BufferedReader(isr);
			String str = null;
			while((str = br.readLine()) != null){
				list.add(str);
			}
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
     }
     return list;
	}
}