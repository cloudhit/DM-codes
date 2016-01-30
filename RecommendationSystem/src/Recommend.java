import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.regex.Pattern;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

class Elem2
{
	int itemID;
	double rating;
	public Elem2()
	{
		
	}
	public Elem2(int id, double sim)
	{
		itemID = id;
		rating = sim;
	}
}

public class Recommend
{
	public static int group = 10;
	public static int recNum = 10;
	public static final Pattern DELIMITER = Pattern.compile("[\t,]");
    public static class MatrixMapper extends Mapper<LongWritable, Text, Text, Text>
    {
        @Override
        public void map(LongWritable key, Text values, Context context) throws IOException, InterruptedException
        {
        	String[] tokens = DELIMITER.split(values.toString());
        	if(tokens[0].startsWith("A"))
        	{
        		double rate = Double.parseDouble(tokens[3]);
        		if(Math.abs(rate) > 1e-6)
        		{
		        	int row = Integer.parseInt(tokens[1]);
		        	int col = Integer.parseInt(tokens[2]);
		            for (int i = 0; i < group; i ++)
		            {
		                Text k = new Text(i + "");
	                    Text v = new Text("A," + row + "," + col + "," + rate);
	                    context.write(k, v);
		            }
        		}
        	}
        	else if(tokens[0].startsWith("R"))
        	{
        		int uID = Integer.parseInt(tokens[1]);
        		int groupIndex = uID % group;
        		Text k = new Text(groupIndex + "");
                context.write(k, values);
        	}
        	else
        	{
        		int uID = Integer.parseInt(tokens[0]);
        		int groupIndex = uID % group;
        		Text k = new Text(groupIndex + "");
                context.write(k, values);
        	}
        }
    }
    
    public static class MatrixReducer extends Reducer<Text, Text, Text, Text>
    {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
        {
        	ArrayList <Map <Integer, Double>> map = new ArrayList <Map <Integer, Double>> (); //normalized ratings
        	for(int i = 0; i <= Reformat.userNum; i ++)
        	{
        		Map <Integer, Double> ins = new HashMap <Integer, Double> ();
        		map.add(ins);
        	}
        	
        	ArrayList <Integer> uID = new ArrayList <Integer> ();
        	ArrayList <Map <Integer, Double>> simMap = new ArrayList <Map <Integer, Double>> (); //similarities
        	Map <Integer, Double> avg = new HashMap <Integer, Double> ();//average ratings
        	
            for (Text line : values)
            {
            	String val = line.toString();
            	if (val.startsWith("A"))
                {
	                String[] kv = DELIMITER.split(val);
	                Integer j = Integer.parseInt(kv[1]);
	                Integer i = Integer.parseInt(kv[2]);
	                Double rate = Double.parseDouble(kv[3]);
	                map.get(i).put(j, rate);
                }
            	else if (val.startsWith("R"))
            	{
            		String[] kv = DELIMITER.split(val);
            		Integer id = Integer.parseInt(kv[1]);
            		Double rate = Double.parseDouble(kv[2]);
            		avg.put(id, rate);
            	}
            	else
            	{
            		String[] kv = DELIMITER.split(val);
            		Integer id = Integer.parseInt(kv[0]);
            		uID.add(id);
            		Map <Integer, Double> ins = new HashMap <Integer, Double> ();
            		for(int i = 0; i < kNN.K; i ++)
            		{
            			Integer Int = Integer.parseInt(kv[2 * i + 1]);
            			Double Dou = Double.parseDouble(kv[2 * i + 2]);
            			ins.put(Int, Dou);
            		}
            		simMap.add(ins);
            	}
            }

            Comparator <Elem2> OrderIsdn = new Comparator <Elem2> ()
        	{
	        	public int compare(Elem2 e1, Elem2 e2)
	        	{
	        		double rate1 = e1.rating;
	        		double rate2 = e2.rating;
	        		if(rate1 > rate2)
	        		{
	        			return 1;
	        		}
	        		else if(rate1 < rate2)
	        		{
	        			return -1;
	        		}
	        		else return 0;
	        	}
        	};
            int size = uID.size();
            for(int i = 0; i < size; i ++)
            {
            	int userID = uID.get(i);
            	Queue <Elem2> recQ = new PriorityQueue <Elem2> (11, OrderIsdn);//recommended items
            	boolean[] visitItem = new boolean[Reformat.itemNum + 1];
            	for(int j = 0; j <= Reformat.itemNum; j ++)
            		visitItem[j] = false;
            	
            	Iterator <Integer> iter = simMap.get(i).keySet().iterator();
            	while(iter.hasNext())
            	{
            		int userID2 = iter.next();
            		Queue <Elem2> queue = new PriorityQueue <Elem2> (11, OrderIsdn);
            		Iterator <Integer> iter2 = map.get(userID2).keySet().iterator();
            		while(iter2.hasNext())
            		{
            			Integer itemID2 = iter2.next();
            			if(!visitItem[itemID2])
            			{
	            			if(!map.get(userID).containsKey(itemID2))
	            			{
	            				Elem2 ele = new Elem2(itemID2, map.get(userID2).get(itemID2));
	            				queue.add(ele);
	            				if(queue.size() > recNum)
	            				{
	            					queue.poll();
	            				}
	            			}
            			}
            		}
            		while(!queue.isEmpty())
            		{
            			Elem2 ele2 = queue.poll();
            			Integer itemID2 = ele2.itemID;
            			visitItem[itemID2] = true;
            			double sumSim = 0.0;
            			double sumRating = 0.0;
            			Iterator <Integer> iter3 = simMap.get(i).keySet().iterator();
            			while(iter3.hasNext())
            			{
            				Integer neighbor = iter3.next();
            				if(map.get(neighbor).containsKey(itemID2))
            				{
            					double rating = map.get(neighbor).get(itemID2);
            					double sim = simMap.get(i).get(neighbor);
            					sumRating += sim * rating;
            					sumSim += sim;
            				}
            			}
            			double preRating = avg.get(userID) + sumRating / sumSim;
            			Elem2 rec = new Elem2(itemID2, preRating);
            			recQ.add(rec);
            			if(recQ.size() > recNum)
            			{
            				recQ.poll();
            			}
            		}
            	}
            	Text k = new Text(userID + "");
            	Elem2 ele = recQ.poll();
            	String itemList = ele.itemID + "," + ele.rating;
            	while(!recQ.isEmpty())
            	{
            		ele = recQ.poll();
            		itemList = ele.itemID + "," + ele.rating + "," + itemList;
            	}
            	context.write(k, new Text(itemList));
            }
        }
    }
    
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException
    {
        Map <String, String> path = new HashMap <String, String>();
        path.put("input1", "svdrec/input1");
        path.put("input2", "svdrec/kNN");
        path.put("input3", "svdrec/avg");
        path.put("output", "svdrec/rec");

        String input1 = path.get("input1");
        String input2 = path.get("input2");
        String input3 = path.get("input3");
        String output = path.get("output");

        Job job = new Job();
        job.setJarByClass(kNN.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setMapperClass(MatrixMapper.class);
        job.setReducerClass(MatrixReducer.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.setInputPaths(job, new Path(input1), new Path(input2), new Path(input3));
        FileOutputFormat.setOutputPath(job, new Path(output));

        job.waitForCompletion(true);
    }
}
