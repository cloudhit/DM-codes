import java.io.IOException;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.Queue;
import java.util.PriorityQueue;
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
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

class Elem
{
	int userID;
	double similarity;
	public Elem()
	{
		
	}
	public Elem(int id, double sim)
	{
		userID = id;
		similarity = sim;
	}
}

public class kNN
{
	public static int K = 10;//the value of K in kNN
	public static int group = 10;
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
	    		
	            for (int i = 0; i < group; i ++)
	            {
	                Text k = new Text(i + "");
                    Text v = new Text(row + "," + col + "," + rate);
                    context.write(k, v);
	            }
        	}
        }
    }

    public static class MatrixReducer extends Reducer<Text, Text, Text, Text>
    {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
        {
        	double[][] matrix = new double[Reformat.userNum][Reformat.k];
        	int groupID = Integer.parseInt(key.toString());

        	Comparator <Elem> OrderIsdn = new Comparator <Elem> ()
        	{
	        	public int compare(Elem e1, Elem e2)
	        	{
	        		double rate1 = e1.similarity;
	        		double rate2 = e2.similarity;
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
        	
            for (Text line : values)
            {
                String val = line.toString();
                String[] kv = DELIMITER.split(val);
                int x = Integer.parseInt(kv[0]);
                int y = Integer.parseInt(kv[1]);
                double rate = Double.parseDouble(kv[2]);
                matrix[x-1][y-1] = rate;
            }
            
            for(int i = 1; i <= Reformat.userNum; i ++)
            {
            	if(i % group == groupID)
            	{
            		Queue <Elem> queue = new PriorityQueue <Elem> (11, OrderIsdn);
	            	for(int j = 1; j <= Reformat.userNum; j ++)
	            	{
	            		if(j == i)
	            			continue;
	            		
	            		double res = 0.0;
		            	double A = 0.0;
		            	double B = 0.0;
	            		for(int k = 1; k <= Reformat.k; k ++)
	            		{
	            			res += matrix[i-1][k-1] * matrix[j-1][k-1];
	            			A += matrix[i-1][k-1] * matrix[i-1][k-1];
	            			B += matrix[j-1][k-1] * matrix[j-1][k-1];
	            		}
	            		res /= Math.sqrt(A) * Math.sqrt(B);
	            		Elem ele = new Elem(j, res);
	            		queue.add(ele);
	            		if(queue.size() > K)
	            		{
	            			queue.poll();
	            		}
	            	}
	            	Text k = new Text(i + "");
	            	Elem ele = queue.poll();
	            	String userList = ele.userID + "," + ele.similarity + "";
	            	for(int j = 2; j <= K; j ++)
	            	{
	            		ele = queue.poll();
	            		userList = ele.userID + "," + ele.similarity + "," + userList;
	            	}
	            	context.write(k, new Text(userList));
            	}
            }
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException
    {
        Map <String, String> path = new HashMap <String, String>();
        path.put("input", "svdrec/outputSVD");
        path.put("output", "svdrec/kNN");

        String input = path.get("input");
        String output = path.get("output");

        Job job = new Job();
        job.setJarByClass(kNN.class);

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
