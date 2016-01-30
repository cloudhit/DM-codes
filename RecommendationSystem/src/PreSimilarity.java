import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
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

public class PreSimilarity
{
	public static int userNum = 943;
	public static int itemNum = 1682;
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
        	
        	ArrayList <String> itemArray = new ArrayList <String> ();
            Map <String, String> map = new HashMap <String, String>();
            int num = 0;
            Double sum = 0.0;
            for (Text line : values)
            {
                String val = line.toString();
                String[] kv = DELIMITER.split(val);
                map.put(kv[0], kv[1]);
                itemArray.add(kv[0]);
                double rate = Double.parseDouble(kv[1]);
                sum += rate;
                num ++;
            }
            double average = sum / num;
            String str = "";
            for(String e: itemArray)
            {
            	double newRate = Double.parseDouble(map.get(e)) - average;
            	str += e + "," + newRate + ",";
            	//context.write(key, new Text(e + "," + newRate));
            }
            context.write(key, new Text(str));
        }
    }
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException, TasteException
	{
		Map <String, String> path = new HashMap <String, String>();
        path.put("input", "exp/input");
        path.put("output", "exp/outputSIM");
        String input = path.get("input");
        String output = path.get("output");

        Job job = new Job();
        job.setJarByClass(PreSimilarity.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setMapperClass(Rec1Mapper.class);
        job.setReducerClass(Rec1Reducer.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.setInputPaths(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));
        job.waitForCompletion(true);
	}
}
