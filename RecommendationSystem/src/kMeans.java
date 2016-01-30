import java.util.*;
import java.io.*;
import java.lang.*;
import java.util.regex.Pattern;
import org.apache.hadoop.*;

public class kMeans{
	public static class kMeansMap extends Mapper<Text, Text, IntWritable, Text>{
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
    public static class kMeansReduceI extends Reducer<IntWritable, Text, IntWritable, Text>{
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
          	String[] coord = tmp.split('&')[1].split(',');
          	double[] point = new double[statistics.dimNum + 1];
          	for(int i = 0; i < statistics.dimNum; i ++)
          		point[i + 1] = Double.parseDouble(coord[i]);
          	double min_v = Double.MAX_VALUE;
          	int min_index = -1;
          	for(int i = 0; i < cur_k; i ++){
                DisMeasure dis = new DisMeasure(centers[i], point);
                if(dis < min_v){
                   min_v = dis;
                   min_index = i;
                }
          	}
          context.write(new IntWritable(min_index), new Text(tmp.split('&')[1]));
          }
    	}   
    }
    public static class kMeansReduceII extends Reducer<IntWritable, Text, Text, Text>{
    	@Override
    	public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
          double[] s = new double[statistics.dimNum + 1];
          Iterator<Text> it = values.iterator();
          int cnt = 0;
          while(it.hasNext()){
            String[] tmp = it.next().toString().split(',');
            for(int i = 0; i < statistics.dimNum; i ++){
            	s[i + 1] += Doubleu.parseDouble(tmp[i]);
            }
            cnt ++;
          }
          StringBuilder sb = new StringBuilder("");
          for(int i = 1; i <= statistics.dimNum; i ++)
            sb.append(String.valueOf(s[i] / cnt) + ",");
          Text out = new Text(sb.toString);
          context.write(new Text("C"), out);
    	}
    }
}