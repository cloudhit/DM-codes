import java.util.*;
import java.io.*;
import java.lang.*;
import java.util.regex.Pattern;
import org.apache.hadoop.*;

public class kMeansDrive{
	public static void main(String[] args) {
	 Configuration conf = new Configuration();
	 int k = 15, reducer_num = 30;
	 for(int i = 1; i <= 50; i ++){
	 	String input11 = "a/input11", input12 = "a/input12";
	 	String output1 = "a/output";
	 	String input2 = output1;
	 	String output2 = input12;
	 	conf.set("K", k);
	 	conf.set("REDUCER_NUM", reducer_num);
	 	Job job1 = new Job(conf, "kMeans1");
	 	job1.setJarByClass(kMeans.class);
	 	job1.setMapperClass(kMeansMap.class);
	 	job1.setReducerClass(kMeansReducerI.class);
	 	job1.setMapOutputKeyClass(IntWritable.class);
	 	job1.setMapOutputValueClass(Text.class);
	 	job1.setOutputKeyClass(IntWritable.class);
	 	job1.setOutputValueClass(Text.class);
	 	FileInputFormat.addInputPaths(job1, new Path(input1), new Path(input2));
		FileOutputFormat.setOutputPath(job1, new Path(output1));
        job1.waitForCompletion(true);
        
        Job job2 = new Job(conf, "kMeans2");
	 	job2.setJarByClass(kMeans.class);
	 	job2.setReducerClass(kMeansReducerII.class);
	 	job2.setOutputKeyClass(Text.class);
	 	job2.setOutputValueClass(Text.class);
	 	FileInputFormat.addInputPaths(job2, new Path(input2));
		FileOutputFormat.setOutputPath(job2, new Path(output2));
        job2.waitForCompletion(true);
	 }
	}
}