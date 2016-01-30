import java.io.IOException;
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
import org.apache.mahout.math.DenseMatrix;
import org.apache.mahout.math.Matrix;
import org.apache.mahout.math.QRDecomposition;
import org.apache.mahout.math.SingularValueDecomposition;

public class RandomizedSVD
{
	public static final Pattern DELIMITER = Pattern.compile("[\t,]");
    public static class Matrix1Mapper extends Mapper<LongWritable, Text, Text, Text>
    {
    	private int rowNum = Reformat.itemNum;
        private int colNum = Reformat.k;
        private int group = 10;
        
        private int rows = rowNum / group;
        private int cols = colNum / group;
        private int rowRemain = rowNum % group;
        private int colRemain = colNum % group;
        
        @Override
        public void map(LongWritable key, Text values, Context context) throws IOException, InterruptedException
        {
        	String[] tokens = DELIMITER.split(values.toString());
        	if (tokens[0].charAt(0) == 'A')
            {
            	int r = Integer.parseInt(tokens[1]);
            	int c = Integer.parseInt(tokens[2]);
            	
            	Integer rGroupNum = r / rows;
            	int rRemain = r % rows;
            	if(rRemain > Math.min(rowRemain, rGroupNum))
            		rGroupNum ++;
            	
            	for (int i = 1; i <= group; i ++)
                {
	                Text k = new Text(rGroupNum.toString() + "," + i);
	                Text v = new Text("A:" + r + "," + c + "," + tokens[3]);
	                context.write(k, v);
                }
            }
        	else if (tokens[0].charAt(0) == 'O')
            {
            	int r = Integer.parseInt(tokens[1]);
            	int c = Integer.parseInt(tokens[2]);
            	
            	Integer cGroupNum = c / cols;
            	int cRemain = c % cols;
            	if(cRemain > Math.min(colRemain, cGroupNum))
            		cGroupNum ++;
            	
            	for (int i = 1; i <= group; i ++)
                {
	                Text k = new Text(i + "," + cGroupNum.toString());
	                Text v = new Text("O:" + r + "," + c + "," + tokens[3]);
	                context.write(k, v);
                }
            }
        }
    }

    public static class Matrix1Reducer extends Reducer<Text, Text, Text, DoubleWritable>
    {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
        {
            Map <String, String> mapA = new HashMap <String, String>();
            Map <String, String> mapB = new HashMap <String, String>();
            int minI = Integer.MAX_VALUE, maxI = -1, minJ = Integer.MAX_VALUE, maxJ = -1, length = -1;

            for (Text line : values)
            {
                String val = line.toString();
                if (val.startsWith("A:"))
                {
                    String[] kv = DELIMITER.split(val.substring(2));
                    mapA.put(kv[0] + "," + kv[1], kv[2]);
                    int x = Integer.parseInt(kv[0]);
                    int y = Integer.parseInt(kv[1]);
                    if(x < minI)
                    	minI = x;
                    if(x > maxI)
                    	maxI = x;
                    if(y > length)
                    	length = y;
                }
                else if (val.startsWith("O:"))
                {
                    String[] kv = DELIMITER.split(val.substring(2));
                    mapB.put(kv[0] + "," + kv[1], kv[2]);
                    int y = Integer.parseInt(kv[1]);
                    if(y < minJ)
                    	minJ = y;
                    if(y > maxJ)
                    	maxJ = y;
                }
            }
            
            for(Integer i = minI; i <= maxI; i ++)
            {
            	for(Integer j = minJ; j <= maxJ; j ++)
                {
		            double result = 0;
		            for(Integer k = 1; k <= length; k ++)
		            {
		            	String double1 = i.toString() + "," + k.toString();
		            	String double2 = k.toString() + "," + j.toString();
		                result += Double.parseDouble(mapA.get(double1)) * Double.parseDouble(mapB.get(double2));
		            }
		            context.write(new Text(i + "," + j), new DoubleWritable(result));
                }
            }
        }
    }
    
    public static class Matrix2Mapper extends Mapper<LongWritable, Text, Text, Text>
    {
    	public void map(LongWritable key, Text values, Context context) throws IOException, InterruptedException
        {
        	String[] tokens = DELIMITER.split(values.toString());
        	
        	int r = Integer.parseInt(tokens[0]);
        	int c = Integer.parseInt(tokens[1]);
        	
            Text k = new Text("1");
            Text v = new Text(r + "," + c + "," + tokens[2]);
            context.write(k, v);
        }
    }

    public static class Matrix2Reducer extends Reducer<Text, Text, Text, DoubleWritable>
    {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
        {
        	double[][] Y = new double[Reformat.itemNum][Reformat.k];
            for (Text line : values)
            {
                String val = line.toString();
                String[] kv = DELIMITER.split(val);
                int x = Integer.parseInt(kv[0]);
                int y = Integer.parseInt(kv[1]);
                double value = Double.parseDouble(kv[2]);
                Y[x-1][y-1] = value;
            }
            
            DenseMatrix matrix = new DenseMatrix(Y);
            QRDecomposition QR = new QRDecomposition(matrix);
            Matrix Q = QR.getQ();
            
            for(Integer i = 0; i < Reformat.itemNum; i ++)
            {
            	for(Integer j = 0; j < Reformat.k; j ++)
                {
		            context.write(new Text("Q," + (i+1) + "," + (j+1)), new DoubleWritable(Q.get(i, j)));
                }
            }
        }
    }
    
    public static class Matrix3Mapper extends Mapper<LongWritable, Text, Text, Text>
    {
    	private int rowNum = Reformat.k;
        private int colNum = Reformat.userNum;
        private int group = 10;
        
        private int rows = rowNum / group;
        private int cols = colNum / group;
        private int rowRemain = rowNum % group;
        private int colRemain = colNum % group;
        
        @Override
        public void map(LongWritable key, Text values, Context context) throws IOException, InterruptedException
        {
        	String[] tokens = DELIMITER.split(values.toString());
        	if (tokens[0].charAt(0) == 'Q')
            {
            	int c = Integer.parseInt(tokens[1]);
            	int r = Integer.parseInt(tokens[2]);
            	
            	Integer rGroupNum = r / rows;
            	int rRemain = r % rows;
            	if(rRemain > Math.min(rowRemain, rGroupNum))
            		rGroupNum ++;
            	
            	for (int i = 1; i <= group; i ++)
                {
	                Text k = new Text(rGroupNum.toString() + "," + i);
	                Text v = new Text("Q:" + r + "," + c + "," + tokens[3]);
	                context.write(k, v);
                }
            }
        	else if (tokens[0].charAt(0) == 'A')
            {
            	int r = Integer.parseInt(tokens[1]);
            	int c = Integer.parseInt(tokens[2]);
            	
            	Integer cGroupNum = c / cols;
            	int cRemain = c % cols;
            	if(cRemain > Math.min(colRemain, cGroupNum))
            		cGroupNum ++;
            	
            	for (int i = 1; i <= group; i ++)
                {
	                Text k = new Text(i + "," + cGroupNum.toString());
	                Text v = new Text("A:" + r + "," + c + "," + tokens[3]);
	                context.write(k, v);
                }
            }
        }
    }

    public static class Matrix3Reducer extends Reducer<Text, Text, Text, DoubleWritable>
    {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
        {
            Map <String, String> mapA = new HashMap <String, String>();
            Map <String, String> mapB = new HashMap <String, String>();
            int minI = Integer.MAX_VALUE, maxI = -1, minJ = Integer.MAX_VALUE, maxJ = -1, length = -1;

            for (Text line : values)
            {
                String val = line.toString();
                if (val.startsWith("Q:"))
                {
                    String[] kv = DELIMITER.split(val.substring(2));
                    mapA.put(kv[0] + "," + kv[1], kv[2]);
                    int x = Integer.parseInt(kv[0]);
                    int y = Integer.parseInt(kv[1]);
                    if(x < minI)
                    	minI = x;
                    if(x > maxI)
                    	maxI = x;
                    if(y > length)
                    	length = y;
                }
                else if (val.startsWith("A:"))
                {
                    String[] kv = DELIMITER.split(val.substring(2));
                    mapB.put(kv[0] + "," + kv[1], kv[2]);
                    int y = Integer.parseInt(kv[1]);
                    if(y < minJ)
                    	minJ = y;
                    if(y > maxJ)
                    	maxJ = y;
                }
            }
            
            for(Integer i = minI; i <= maxI; i ++)
            {
            	for(Integer j = minJ; j <= maxJ; j ++)
                {
		            double result = 0;
		            for(Integer k = 1; k <= length; k ++)
		            {
		            	String double1 = i.toString() + "," + k.toString();
		            	String double2 = k.toString() + "," + j.toString();
		                result += Double.parseDouble(mapA.get(double1)) * Double.parseDouble(mapB.get(double2));
		            }
		            context.write(new Text(i + "," + j), new DoubleWritable(result));
                }
            }
        }
    }
    
    public static class Matrix4Mapper extends Mapper<LongWritable, Text, Text, Text>
    {
    	public void map(LongWritable key, Text values, Context context) throws IOException, InterruptedException
        {
        	String[] tokens = DELIMITER.split(values.toString());
        	
        	int r = Integer.parseInt(tokens[0]);
        	int c = Integer.parseInt(tokens[1]);
        	
            Text k = new Text("1");
            Text v = new Text(r + "," + c + "," + tokens[2]);
            context.write(k, v);
        }
    }

    public static class Matrix4Reducer extends Reducer<Text, Text, Text, DoubleWritable>
    {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
        {
        	double[][] Y = new double[Reformat.k][Reformat.userNum];

            for (Text line : values)
            {
                String val = line.toString();
                
                String[] kv = DELIMITER.split(val);
                int x = Integer.parseInt(kv[0]);
                int y = Integer.parseInt(kv[1]);
                double value = Double.parseDouble(kv[2]);
                Y[x-1][y-1] = value;
            }
            
            DenseMatrix matrix = new DenseMatrix(Y);
        	SingularValueDecomposition SVD = new SingularValueDecomposition(matrix);
        	Matrix U = SVD.getU();
        	Matrix V = SVD.getV();
        	double[] eigenValues = SVD.getSingularValues();
        	
        	for(int i = 0; i < Reformat.k; i ++)
        	{
        		for(int j = 0; j < Reformat.k; j ++)
        		{
        			context.write(new Text("U," + (i+1) + "," + (j+1)), new DoubleWritable(U.get(i, j)));
        		}
        	}
            
        	for(int i = 0; i < Reformat.userNum; i ++)
        	{
        		for(int j = 0; j < Reformat.k; j ++)
        		{
        			context.write(new Text("V," + (i+1) + "," + (j+1)), new DoubleWritable(V.get(i, j)));
        		}
        	}
            
        	for(int i = 0; i < Reformat.k; i ++)
        	{
        		context.write(new Text("E," + (i+1)), new DoubleWritable(eigenValues[i]));
        	}
        }
    }
    
    public static class Matrix5Mapper extends Mapper<LongWritable, Text, Text, Text>
    {
    	private int rowNum = Reformat.itemNum;
        private int colNum = Reformat.k;
        private int group = 10;
        
        private int rows = rowNum / group;
        private int cols = colNum / group;
        private int rowRemain = rowNum % group;
        private int colRemain = colNum % group;
        
        @Override
        public void map(LongWritable key, Text values, Context context) throws IOException, InterruptedException
        {
        	String[] tokens = DELIMITER.split(values.toString());
        	if (tokens[0].charAt(0) == 'Q')
            {
            	int r = Integer.parseInt(tokens[1]);
            	int c = Integer.parseInt(tokens[2]);
            	
            	Integer rGroupNum = r / rows;
            	int rRemain = r % rows;
            	if(rRemain > Math.min(rowRemain, rGroupNum))
            		rGroupNum ++;
            	
            	for (int i = 1; i <= group; i ++)
                {
	                Text k = new Text(rGroupNum.toString() + "," + i);
	                Text v = new Text("Q:" + r + "," + c + "," + tokens[3]);
	                context.write(k, v);
                }
            }
        	else if (tokens[0].charAt(0) == 'U')
            {
            	int r = Integer.parseInt(tokens[1]);
            	int c = Integer.parseInt(tokens[2]);
            	
            	Integer cGroupNum = c / cols;
            	int cRemain = c % cols;
            	if(cRemain > Math.min(colRemain, cGroupNum))
            		cGroupNum ++;
            	
            	for (int i = 1; i <= group; i ++)
                {
	                Text k = new Text(i + "," + cGroupNum.toString());
	                Text v = new Text("U:" + r + "," + c + "," + tokens[3]);
	                context.write(k, v);
                }
            }
        	else if (tokens[0].charAt(0) == 'E')
            {
            	int c = Integer.parseInt(tokens[1]);
            	double value = Double.parseDouble(tokens[2]);
            	
                Text k = new Text("E");
                Text v = new Text(c + "," + value);
                context.write(k, v);
            }
        	else if (tokens[0].charAt(0) == 'V')
            {
            	int r = Integer.parseInt(tokens[1]);
            	int c = Integer.parseInt(tokens[2]);
            	double value = Double.parseDouble(tokens[3]);
            	
                Text k = new Text("V");
                Text v = new Text(r + "," + c + "," + value);
                context.write(k, v);
            }
        }
    }

    public static class Matrix5Reducer extends Reducer<Text, Text, Text, DoubleWritable>
    {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
        {
        	if(key.charAt(0) == 'E')
        	{
        		for (Text line : values)
	            {
        			String val = line.toString();
        			String[] kv = DELIMITER.split(val);
        			double Value = Double.parseDouble(kv[1]);
        			context.write(new Text("E," + kv[0]), new DoubleWritable(Value));
	            }
        	}
        	else if(key.charAt(0) == 'V')
        	{
        		for (Text line : values)
	            {
        			String val = line.toString();
        			String[] kv = DELIMITER.split(val);
        			double Value = Double.parseDouble(kv[2]);
        			context.write(new Text("V," + kv[0] + "," + kv[1]), new DoubleWritable(Value));
	            }
        	}
        	else
        	{
	            Map <String, String> mapA = new HashMap <String, String>();
	            Map <String, String> mapB = new HashMap <String, String>();
	            int minI = Integer.MAX_VALUE, maxI = -1, minJ = Integer.MAX_VALUE, maxJ = -1, length = -1;
	
	            for (Text line : values)
	            {
	                String val = line.toString();
	                if (val.startsWith("Q:"))
	                {
	                    String[] kv = DELIMITER.split(val.substring(2));
	                    mapA.put(kv[0] + "," + kv[1], kv[2]);
	                    int x = Integer.parseInt(kv[0]);
	                    int y = Integer.parseInt(kv[1]);
	                    if(x < minI)
	                    	minI = x;
	                    if(x > maxI)
	                    	maxI = x;
	                    if(y > length)
	                    	length = y;
	                }
	                else if (val.startsWith("U:"))
	                {
	                    String[] kv = DELIMITER.split(val.substring(2));
	                    mapB.put(kv[0] + "," + kv[1], kv[2]);
	                    int y = Integer.parseInt(kv[1]);
	                    if(y < minJ)
	                    	minJ = y;
	                    if(y > maxJ)
	                    	maxJ = y;
	                }
	            }
	            
	            for(Integer i = minI; i <= maxI; i ++)
	            {
	            	for(Integer j = minJ; j <= maxJ; j ++)
	                {
			            double result = 0;
			            for(Integer k = 1; k <= length; k ++)
			            {
			            	String double1 = i.toString() + "," + k.toString();
			            	String double2 = k.toString() + "," + j.toString();
			                result += Double.parseDouble(mapA.get(double1)) * Double.parseDouble(mapB.get(double2));
			            }
			            context.write(new Text("U," + i + "," + j), new DoubleWritable(result));
	                }
	            }
        	}
        }
    }
    
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException
	{
		Map <String, String> path = new HashMap <String, String>();
        path.put("input1", "exp/input1");
        path.put("input2", "exp/input2");
        path.put("output", "exp/output");
        String input1 = path.get("input1");
        String input2 = path.get("input2");
        String output = path.get("output");

        Job job = new Job();
        job.setJarByClass(RandomizedSVD.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setMapperClass(Matrix1Mapper.class);
        job.setReducerClass(Matrix1Reducer.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.setInputPaths(job, new Path(input1), new Path(input2));
        FileOutputFormat.setOutputPath(job, new Path(output));
        job.waitForCompletion(true);
		
        String input = output;
        output = "exp/output2";
        job = new Job();
        job.setJarByClass(RandomizedSVD.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setMapperClass(Matrix2Mapper.class);
        job.setReducerClass(Matrix2Reducer.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.setInputPaths(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));
        job.waitForCompletion(true);
        
        input2 = input1;
        input1 = output;
        output = "exp/output3";
        job = new Job();
        job.setJarByClass(RandomizedSVD.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setMapperClass(Matrix3Mapper.class);
        job.setReducerClass(Matrix3Reducer.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.setInputPaths(job, new Path(input1), new Path(input2));
        FileOutputFormat.setOutputPath(job, new Path(output));
        job.waitForCompletion(true);
        
        input = output;
        output = "exp/output4";
        job = new Job();
        job.setJarByClass(RandomizedSVD.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setMapperClass(Matrix4Mapper.class);
        job.setReducerClass(Matrix4Reducer.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.setInputPaths(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));
        job.waitForCompletion(true);
        
        input1 = "exp/output2";
        input2 = output;
        output = "exp/outputSVD";
        job = new Job();
        job.setJarByClass(RandomizedSVD.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setMapperClass(Matrix5Mapper.class);
        job.setReducerClass(Matrix5Reducer.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.setInputPaths(job, new Path(input1), new Path(input2));
        FileOutputFormat.setOutputPath(job, new Path(output));
        job.waitForCompletion(true);
	}
}
