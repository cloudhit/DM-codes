import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.regex.Pattern;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.mahout.cf.taste.common.TasteException;
import org.mortbay.log.Log;

public class Similarity
{
	public static int userNum = Reformat.userNum;
	public static int itemNum = Reformat.itemNum;
	public static final Pattern DELIMITER1 = Pattern.compile("[\t,]");
	public static final Pattern DELIMITER = Pattern.compile("\\t");
    public static class Rec1Mapper1 extends Mapper<LongWritable, Text, Text, Text>
    {
        @Override
        public void map(LongWritable key, Text values, Context context) throws IOException, InterruptedException
        {
        	String[] tokens = DELIMITER.split(values.toString());
        	int userID = Integer.parseInt(tokens[0]);
        	Text v = new Text(userID + "," + tokens[1]);
            for(int i = userID + 1; i <= userNum; i ++)
            {
                Text k = new Text(userID + "," + i);
            	context.write(k, v);
            }
            for(int i = 1; i < userID; i ++)
            {
            	Text k=new Text(i+","+userID);
            	context.write(k, v);
	        }
	    }
    }

    public static class Rec1Reducer1 extends Reducer<Text, Text, Text, Text>
    {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
        {
            ArrayList <Double> map = new ArrayList <Double> ();
            int[] index = new int[itemNum];
            for(int i = 0; i < itemNum; i ++)
            	index[i] = -1;
            
            double sum = 0.0;
            boolean visit = false;
            double a=0.0;
            double b=0.0;
            int counter = 0;
            for (Text line : values)
            {
            	 String val = line.toString();
            	 String[] kv = DELIMITER1.split(val);
                 counter++;
            	if(!visit)
            	{
            		for (int i=1;i<kv.length;i=i+2)
            		{
            			map.add(Double.parseDouble(kv[i+1]));
            			index[Integer.parseInt(kv[i])-1] = i/2;
            		}
            		visit=true;
            	}
            	else
            	{
            		for (int i=1;i<kv.length;i=i+2)
            		{
                		double value= Double.parseDouble(kv[i+1]);
                		int cur = index[Integer.parseInt(kv[i])-1];
                		if(cur != -1)
            			{
            				double rate=map.get(cur);
            				sum+=rate*value;
            				a+=rate*rate;
            				b+=value*value;
            			}
            		}
            	}
            }
            double similar=sum/(Math.sqrt(a)*Math.sqrt(b));
            context.write(new Text("S,"+key), new Text(similar+""));
        }
    }
	

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException, TasteException
	{
		Map <String, String> path = new HashMap <String, String>();
	    path.put("input", "exp/outputSIM");
	    path.put("output", "exp/sim");
	    String input = path.get("input");
	    String output = path.get("output");
	    
	    Job job = new Job();
	    job.setJarByClass(Similarity.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);
	    job.setMapperClass(Rec1Mapper1.class);
	    job.setReducerClass(Rec1Reducer1.class);
	    job.setInputFormatClass(TextInputFormat.class);
	    job.setOutputFormatClass(TextOutputFormat.class);
	    FileInputFormat.setInputPaths(job, new Path(input));
	    FileOutputFormat.setOutputPath(job, new Path(output));
	    job.waitForCompletion(true);
	}
}
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.regex.Pattern;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.mahout.cf.taste.common.TasteException;
import org.mortbay.log.Log;

public class Similarity
{
    public static int userNum = Reformat.userNum;
    public static int itemNum = Reformat.itemNum;
    public static final Pattern DELIMITER1 = Pattern.compile("[\t,]");
    public static final Pattern DELIMITER = Pattern.compile("\\t");
    public static class Rec1Mapper1 extends Mapper<LongWritable, Text, Text, Text>
    {
        @Override
        public void map(LongWritable key, Text values, Context context) throws IOException, InterruptedException
        {
            String[] tokens = DELIMITER.split(values.toString());
            int userID = Integer.parseInt(tokens[0]);
            Text v = new Text(userID + "," + tokens[1]);
            for(int i = userID + 1; i <= userNum; i ++)
            {
                Text k = new Text(userID + "," + i);
                context.write(k, v);
            }
            for(int i = 1; i < userID; i ++)
            {
                Text k=new Text(i+","+userID);
                context.write(k, v);
            }
        }
    }

    public static class Rec1Reducer1 extends Reducer<Text, Text, Text, Text>
    {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
        {
            ArrayList <Double> map = new ArrayList <Double> ();
            int[] index = new int[itemNum];
            for(int i = 0; i < itemNum; i ++)
                index[i] = -1;
            
            double sum = 0.0;
            boolean visit = false;
            double a=0.0;
            double b=0.0;
            int counter = 0;
            for (Text line : values)
            {
                 String val = line.toString();
                 String[] kv = DELIMITER1.split(val);
                 counter++;
                if(!visit)
                {
                    for (int i=1;i<kv.length;i=i+2)
                    {
                        map.add(Double.parseDouble(kv[i+1]));
                        index[Integer.parseInt(kv[i])-1] = i/2;
                    }
                    visit=true;
                }
                else
                {
                    for (int i=1;i<kv.length;i=i+2)
                    {
                        double value= Double.parseDouble(kv[i+1]);
                        int cur = index[Integer.parseInt(kv[i])-1];
                        if(cur != -1)
                        {
                            double rate=map.get(cur);
                            sum+=rate*value;
                            a+=rate*rate;
                            b+=value*value;
                        }
                    }
                }
            }
            double similar=sum/(Math.sqrt(a)*Math.sqrt(b));
            context.write(new Text("S,"+key), new Text(similar+""));
        }
    }
    

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException, TasteException
    {
        Map <String, String> path = new HashMap <String, String>();
        path.put("input", "exp/outputSIM");
        path.put("output", "exp/sim");
        String input = path.get("input");
        String output = path.get("output");
        
        Job job = new Job();
        job.setJarByClass(Similarity.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setMapperClass(Rec1Mapper1.class);
        job.setReducerClass(Rec1Reducer1.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.setInputPaths(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));
        job.waitForCompletion(true);
    }
}
