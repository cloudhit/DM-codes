import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
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
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

class Elem3
{
	int itemID;
	double rating;
	public Elem3()
	{
		
	}
	public Elem3(int id, double sim)
	{
		itemID = id;
		rating = sim;
	}
}

public class MyRecommend
{
	public static int group = 10;
	public static int recNum = 10;
	public static int clusterNum = 100;
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
        	else if(tokens[0].startsWith("S"))
        	{
        		int x = Integer.parseInt(tokens[1]) % group;
        		int y = Integer.parseInt(tokens[2]) % group;
        		if(tokens[3].startsWith("N"))
        			tokens[3] = "0.0";
        		Text k = new Text(x + "");
        		String str = "S," + tokens[1] + "," + tokens[2] + "," + tokens[3];
        		context.write(k, new Text(str));
        		k = new Text(y + "");
        		str = "S," + tokens[2] + "," + tokens[1] + "," + tokens[3];
        		context.write(k, new Text(str));
        	}
        	else
        	{
        		for(int groupIndex = 0; groupIndex < group; groupIndex ++)
        		{
	        		Text k = new Text(groupIndex + "");
	                context.write(k, values);
        		}
        	}
        }
    }
    
    public static class MatrixReducer extends Reducer<Text, Text, Text, Text>
    {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
        {
        	//A
        	ArrayList <Map <Integer, Double>> map = new ArrayList <Map <Integer, Double>> (); //normalized ratings
        	for(int i = 0; i <= Reformat.userNum; i ++)
        	{
        		Map <Integer, Double> ins = new HashMap <Integer, Double> ();
        		map.add(ins);
        	}
        	//R
        	Map <Integer, Double> avg = new HashMap <Integer, Double> ();//average ratings
        	//S
        	Map <Integer, HashMap<Integer, Double>> sim = new HashMap <Integer, HashMap<Integer, Double>> ();//similarities
        	//
        	ArrayList <HashSet <Integer>> cluster = new ArrayList <HashSet <Integer>> ();
        	for(int i = 0; i < clusterNum; i ++)
        	{
        		HashSet <Integer> temp = new HashSet <Integer> ();
        		cluster.add(temp);
        	}
        	
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
            	else if (val.startsWith("S"))
            	{
            		String[] kv = DELIMITER.split(val);
            		Integer x = Integer.parseInt(kv[1]);
            		if(!sim.containsKey(x))
            		{
            			HashMap <Integer, Double> temp = new HashMap <Integer, Double> ();
            			sim.put(x, temp);
            		}
            		Integer y = Integer.parseInt(kv[2]);
            		Double simi = Double.parseDouble(kv[3]);
            		sim.get(x).put(y, simi);
            	}
            	else
            	{
            		String[] kv = DELIMITER.split(val);
            		Integer id = Integer.parseInt(kv[0]);
            		for(int i = 1; i < kv.length; i ++)
            		{
            			if(kv[i].length() > 0)
            			{
            				cluster.get(id).add(Integer.parseInt(kv[i]));
            			}
            		}
            	}
            }

            Comparator <Elem3> OrderIsdn = new Comparator <Elem3> ()
        	{
	        	public int compare(Elem3 e1, Elem3 e2)
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
        	
        	Iterator <Integer> i = avg.keySet().iterator();
        	while(i.hasNext())
            {
            	Integer userID = i.next();
            	Queue <Elem3> recQ = new PriorityQueue <Elem3> (11, OrderIsdn);//recommended items
            	boolean[] visitItem = new boolean[Reformat.itemNum + 1];
            	for(int j = 0; j <= Reformat.itemNum; j ++)
            		visitItem[j] = false;
            	
            	int clusterID = -1;
            	for(int j = 0; j < clusterNum; j ++)
            	{
            		if(cluster.get(j).contains(userID))
            		{
            			clusterID = j;
            			break;
            		}
            	}
            	
            	if(clusterID != -1)
            	{
	            	Iterator <Integer> iter = cluster.get(clusterID).iterator();
	            	while(iter.hasNext())
	            	{
	            		int userID2 = iter.next();
	            		if(userID == userID2)
	            			continue;
	            		Queue <Elem3> queue = new PriorityQueue <Elem3> (11, OrderIsdn);
	            		Iterator <Integer> iter2 = map.get(userID2).keySet().iterator();
	            		while(iter2.hasNext())
	            		{
	            			Integer itemID2 = iter2.next();
	            			if(!visitItem[itemID2])
	            			{
		            			if(!map.get(userID).containsKey(itemID2))
		            			{
		            				Elem3 ele = new Elem3(itemID2, map.get(userID2).get(itemID2));
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
	            			Elem3 ele2 = queue.poll();
	            			Integer itemID2 = ele2.itemID;
	            			visitItem[itemID2] = true;
	            			double sumSim = 0.0;
	            			double sumRating = 0.0;
	            			Iterator <Integer> iter3 = sim.get(userID).keySet().iterator();
	            			while(iter3.hasNext())
	            			{
	            				Integer neighbor = iter3.next();
	            				if(map.get(neighbor).containsKey(itemID2))
	            				{
	            					double rating = map.get(neighbor).get(itemID2);
	            					double simi = sim.get(userID).get(neighbor);
	            					if(simi > 0.15)
	            					{
	            					sumRating += simi * rating;
	            					sumSim += Math.abs(simi);
	            					//if(userID == 1)
	            					//	System.out.println(rating + "," + simi + "," + sumRating + "," + sumSim);
	            					}
	            				}
	            			}
	            			Double preRating = avg.get(userID) + sumRating / sumSim;
	            			//if(userID == 1)
        					//	System.out.println(preRating + "," + avg.get(userID) + "," + sumRating + "," + sumSim);
	            			Elem3 rec = new Elem3(itemID2, preRating);
	            			if(!preRating.toString().startsWith("N"))
	            			{
		            			recQ.add(rec);
		            			if(recQ.size() > recNum)
		            			{
		            				recQ.poll();
		            			}
	            			}
	            		}
	            	}
	            	if(!recQ.isEmpty())
	            	{
		            	Text k = new Text(userID + "");
		            	Elem3 ele = recQ.poll();
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
        }
    }
    
	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException
    {
        Map <String, String> path = new HashMap <String, String>();
        path.put("input1", "exp/input1");//A
        path.put("input2", "exp/input3");
        path.put("input3", "exp/avg");//R
        path.put("input4", "exp/sim");//S
        path.put("output", "exp/myrec");

        String input1 = path.get("input1");
        String input2 = path.get("input2");
        String input3 = path.get("input3");
        String input4 = path.get("input4");
        String output = path.get("output");

        Job job = new Job();
        job.setJarByClass(MyRecommend.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setMapperClass(MatrixMapper.class);
        job.setReducerClass(MatrixReducer.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.setInputPaths(job, new Path(input1), new Path(input2), new Path(input3), new Path(input4));
        FileOutputFormat.setOutputPath(job, new Path(output));

        job.waitForCompletion(true);
    }
}
