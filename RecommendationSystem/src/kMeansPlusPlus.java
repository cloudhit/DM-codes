import java.util.*;
import java.io.*;
import java.lang.*;
import java.util.regex.Pattern;
import org.apache.hadoop.*;

public class kMeansPlusPlus{
    /*
     * @author Yupeng Zhang 
    */
    public static class kMeansPlusPlusMap extends Mapper<Text,Text,IntWritable, Text>{
    	@Override
        public void map(Text key, Text value, Context context) throws IOException, InterruptedException{
         int reducer_num = Integer.parseInt(context.getConfiguration().get("REDUCER_NUM"));
         String str = key.toString();
         int id = Integer.parseInt(str);
         if(str.charAt(0) == 'C'){
         	for(int i = 0; i < reducer_num; i ++){
         		context.write(new IntWritable(i), new Text("C&" + value.toString()));
         	}
         }else
            context.write(new IntWritable(id % reducer_num), new Text(id + "&" + value.toString()));
        }
    }
    public static class kMeansPlusPlusReduce extends Reducer<IntWritable, Text, Text, Text>{
    	@Override
    	public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
          Iterator<Text> it = values.iterator();
          int cur_k = Integer.parseInt(context.getConfiguration().get("K"));
          double[][] centers = new double[cur_k][statistics.dimNum + 1]; 
          int cnt = 0;
          while(it.hasNext()){
          	String tmp = it.next().toString();
          	if(tmp.charAt(0) == 'C'){
          		String[] coord = tmp.split('&')[1].split(',');
          		for(int i = 0; i < statistics.dimNum; i ++)
                   centers[cnt][i + 1] = Double.parseDouble(coord[i]); 
               cnt ++;
               if(cnt == cur_k)
               	break;
          	}
          }
          Iterator<Text> it1 = values.iterator();
          while(it1.hasNext()){
          	String tmp = it1.next().toString();
          	if(tmp.charAt(0) == 'C')
          		continue;
          	String id = tmp.split('&')[0];
          	String[] coord = tmp.split('&')[1].split(',');
          	double[] point = new double[statistics.dimNum + 1];
          	for(int i = 0; i < statistics.dimNum; i ++)
          		point[i + 1] = Double.parseDouble(coord[i]);
          	double min_v = Double.MAX_VALUE;
          	for(int i = 0; i < cur_k; i ++){
                DisMeasure dis = new DisMeasure(centers[i], point);
                min_v = Math.min(min_v, dis);
          	}
          	context.write(new Text("1"), new Text(id + "&" + String.valueOf(min_v)));
          }
    	}
    }
    public static class kMeansPlusPlusOutput extends Reducer<Text, Text, Text, Text>{
    	@Override
    	public void reduce(Text key, Iterable<Text> values, Context context){
    		double sum = 0.0;
    		Iterator<Text> it = values.iterator();
    		int[] user = new int[statistics.userNum];
    		double[] dis = new double[statistics.userNum];
    		int cnt = 0;
    		while(it.hasNext()){
                String[] str = it.next().toString().split('&');
                user[cnt] = Integer.parseInt(str[0]);
                double dis_c = Math.pow(Double.parseDouble(str[1]),2);
                dis[cnt ++] = dis_c;
                sum += dis_c;
    		}
    		double ran = Math.random() * sum;
    		for(int i = 0；i < statistics.userNum; i ++){
    			if(ran < dis[i]){
    				context.write(new Text("C" + user[i]), new Text(""));
    				break;
    			}else
    			   ran -= dis[i];
    		}
    	}
    }
    public static class kMeansPlusPlusSearchMap extends Mapper<Text, Text, Text, Text>{
      @Override
      public void map(Text key, Text value, Context context) throws IOException, InterruptedException{
         String str = key.toString();
         int id = Integer.parseInt(key.toString());
         int reducer_num = Integer.parseInt(context.getConfiguration().get("REDUCER_NUM"));
         if(str.charAt(0) == 'C'){
             for(int i = 0; i < reducer_num; i ++)
             	context.write(new Text(String.valueOf(i)), new Text(str + "&");
         }else
            context.write(new Text(String.valueOf(id % reducer_num)), new Text(String.valueOf(id + "&" + value.toString())));
      }
    }
    public static class kMeansPlusPlusSearchReduce extends Reducer<Text, Text, Text, Text>{
     @Override
     public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
        Iterator<Text> it = values.iterator();
        String target = null;
        while(it.hasNext()){
        	String value = it.next().toString();
        	if(value.charAt(0) == 'C'){
               target = value.split('&')[0].substring(1);
               break;
        	}
        }
        Iterator<Text> it1 = values.iterator();
        while(it.hasNext()){
        	String[] tmp = it.next().toString().split('&');
            if(tmp[0].equals(target)){
            	context.write(new Text("C" + target), new Text(tmp[1]));
            	break;
            }
        }
     }
    }
}