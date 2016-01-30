import java.io.IOException;
import java.util.Comparator;
import java.util.HashMap;
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
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class PreKMeans
{
	public static final Pattern DELIMITER = Pattern.compile("[\t,]");
    public static class MatrixMapper extends Mapper<LongWritable, Text, Text, Text>
    {
        @Override
        public void map(LongWritable key, Text values, Context context) throws IOException, InterruptedException
        {
        	String[] tokens = DELIMITER.split(values.toString());
        	if(tokens[0].startsWith("V"))
        	{
	        	int row = Integer.parseInt(tokens[1]);
	        	int col = Integer.parseInt(tokens[2]);
	        	double rate = Double.parseDouble(tokens[3]);
	    		
	            Text k = new Text(row + "");
                Text v = new Text(col + "," + rate);
                context.write(k, v);
        	}
        }
    }

    public static class MatrixReducer extends Reducer<Text, Text, Text, Text>
    {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
        {
        	Double[] vec = new Double[Reformat.k+1];
        	Integer userID = Integer.parseInt(key.toString());
        	double length = 0.0;
            for (Text line : values)
            {
                String val = line.toString();
                String[] kv = DELIMITER.split(val);
                int x = Integer.parseInt(kv[0]);
                double y = Double.parseDouble(kv[1]);
                vec[x] = y;
                length += y * y;
            }
            if(Math.abs(length) > 1e-5)
            {
	            length = 1.0 / Math.sqrt(length);
	            String str = "";
	            for(int i = 1; i <= Reformat.k; i ++)
	            {
	            	vec[i] *= length;
	            	str += vec[i].toString() + ",";
	            }
	            context.write(new Text(userID.toString()), new Text(str));
            }
            else
            {
            	String str = "";
            	Double newRate = Math.sqrt(1.0 / Reformat.k);
            	for(int i = 1; i <= Reformat.k; i ++)
	            {
	            	str += newRate.toString() + ",";
	            }
	            context.write(new Text(userID.toString()), new Text(str));
            }
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException
    {
        Map <String, String> path = new HashMap <String, String>();
        path.put("input", "exp/outputSVD");
        path.put("output", "exp/kMeansInput");

        String input = path.get("input");
        String output = path.get("output");

        Job job = new Job();
        job.setJarByClass(PreKMeans.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setMapperClass(MatrixMapper.class);
        job.setReducerClass(MatrixReducer.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.setInputPaths(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));

        job.waitForCompletion(true);
    }
}
