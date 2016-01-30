import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
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

public class Reformat
{
	public static int userNum = 9999;
	public static int itemNum = 925;
	public static int k = 50;
	public static final Pattern DELIMITER = Pattern.compile("[\t,]");
	
    public static class Rec1Mapper extends Mapper<LongWritable, Text, Text, Text>
    {
        @Override
        public void map(LongWritable key, Text values, Context context) throws IOException, InterruptedException
        {
        	String[] tokens = DELIMITER.split(values.toString());
        	int userID = Integer.parseInt(tokens[0]);
        	int itemID = Integer.parseInt(tokens[1]);
        	double rate = Double.parseDouble(tokens[2]);
        	
            Text k = new Text(userID + "");
            Text v = new Text(itemID + "," + rate);
            context.write(k, v);
        }
    }

    public static class Rec1Reducer extends Reducer<Text, Text, Text, Text>
    {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
        {
        	ArrayList <Integer> itemArray = new ArrayList <Integer> ();
            Map <Integer, Double> map = new HashMap <Integer, Double>();
            int num = 0;
            Double sum = 0.0;
            for (Text line : values)
            {
                String val = line.toString();
                String[] kv = DELIMITER.split(val);
                Integer itemID = Integer.parseInt(kv[0]);
                Double rate = Double.parseDouble(kv[1]);
                map.put(itemID, rate);
                itemArray.add(itemID);
                sum += rate;
                num ++;
            }

            double average = sum / num;
            for(Integer i = 1; i <= itemNum; i ++)
            {
            	if(map.get(i) != null)
            	{
            		Double newRate = map.get(i) - average;
            		String newKey = "A," + i + "," + key.toString();
            		context.write(new Text(newKey), new Text(newRate.toString()));
            	}
            	else
            	{
            		String newKey = "A," + i + "," + key.toString();
            		context.write(new Text(newKey), new Text("0.0"));
            	}
            }
        }
    }
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException
	{
		Map <String, String> path = new HashMap <String, String>();
        path.put("input", "exp/input");
        path.put("output", "exp/input1");
        String input = path.get("input");
        String input1 = path.get("output");

        Job job = new Job();
        job.setJarByClass(Reformat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setMapperClass(Rec1Mapper.class);
        job.setReducerClass(Rec1Reducer.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.setInputPaths(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(input1));
        job.waitForCompletion(true);
	}
}
