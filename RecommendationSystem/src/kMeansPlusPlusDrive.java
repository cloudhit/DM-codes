import java.util.*;
import java.io.*;
import java.lang.*;
import java.util.regex.Pattern;
import org.apache.hadoop.*;
public class kMeansPlusPlusDrive{
    public static void main(String[] args) {
    	Configuration conf = new Configuration();
	    
        int k = 15, reducer_num = 30;
        for(int i = 1; i <= 15; i ++){
	     String input11 = "a/input11", input12 = "a/input12";
	     String output1 = "a/output1";
	     String input2 = output1;
	     String output2 = "a/output2";
	     String input31 = output2, input32 = input11;
         String output3 = input12;
	     conf.set("K", i);
	     conf.set("REDUCER_NUM", reducer_num);
	     Job job1 = new Job(conf, "kMeansPlusPlus1");
	     job1.setJarByClass(kMeansPlusPlus.class);
	     job1.setMapperClass(kMeansPlusPlusMap.class);
	     job1.setReducerClass(kMeansPlusPlusReduce.class);
		 job1.setMapOutputKeyClass(IntWritable.class);
		 job1.setMapOutputValueClass(Text.class);
		 job1.setOutputKeyClass(Text.class);
		 job1.setOutputValueClass(Text.class);  
		 job1.setInputFormatClass(KeyValueTextInputFormat.class);
         FileInputFormat.addInputPaths(job1, new Path(input11), new Path(input12));
		 FileOutputFormat.setOutputPath(job1, new Path(output1));
         job1.waitForCompletion(true);

         Job job2 = new Job(conf, "kMeansPlusPlus2");
         job2.setJarByClass(kMeansPlusPlus.class);
	     job2.setReducerClass(kMeansPlusPlusOutput.class);
		 job2.setOutputKeyClass(Text.class);
		 job2.setOutputValueClass(Text.class);  
		 job2.setInputFormatClass(KeyValueTextInputFormat.class);
         FileInputFormat.addInputPaths(job2, new Path(input2));
		 FileOutputFormat.setOutputPath(job2, new Path(output2));
         job2.waitForCompletion(true);

         Job job3 = new Job(conf, "kMeansPlusPlus3");
	     job3.setJarByClass(kMeansPlusPlus.class);
	     job3.setMapperClass(kMeansPlusPlusSearchMap.class);
	     job3.setReducerClass(kMeansPlusPlusSearchReduce.class);
		 job3.setMapOutputKeyClass(Text.class);
		 job3.setMapOutputValueClass(Text.class);
		 job3.setOutputKeyClass(Text.class);
		 job3.setOutputValueClass(Text.class);  
		 job3.setInputFormatClass(KeyValueTextInputFormat.class);
         FileInputFormat.addInputPaths(job3, new Path(input31), new Path(input32));
		 FileOutputFormat.setOutputPath(job3, new Path(output3));
         job3.waitForCompletion(true);
         
		}
	}
}