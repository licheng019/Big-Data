package vbu.datatribe.hadoopTest;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/*
 * This pattern includes three parts
 * 1. Mapper (map)
 * 2. Reducer (reduce)
 * 3. Driver (join job)
 */
public class MapReducePattern {
	/*
	 * Step1 Mapper Class
	 * Mapper<KEYIN, VALUEIN, KEYOUT,VALUEOUT>
	 * Change Input and Output Type based on your needs
	 */
	
	public static class MapReduceMapper extends Mapper<LongWritable, Text, Text, IntWritable>{

		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			
		}
		
	}
	
	/*
	 * Step2 Reducer Class
	 * Reducer<KEYIN, VALUEIN, KEYOUT,VALUEOUT>
	 * Change Input and Output Type based on your needs
	 */
	public static class MapReduceReducer extends Reducer<Text, IntWritable, Text, IntWritable>{

		@Override
		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			
		}
		
	}
	
	//Step3 Driver
	public int run(String[] args) throws Exception {
		//Get Configuration Information
		Configuration configuration = new Configuration();
		//The second parameter is Job Name
		Job job = Job.getInstance(configuration, this.getClass().getSimpleName());
		//This is the entrance for MapReduce
		job.setJarByClass(this.getClass());
		
		//Set up job
		//1.Input path
		Path input = new Path(args[0]);
		FileInputFormat.addInputPath(job, input);
		
		//2.Output path
		Path output = new Path(args[1]);
		FileOutputFormat.setOutputPath(job, output);
		
		//3.Set Mapper
		job.setMapperClass(MapReduceMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		//4.Set Reducer
		job.setReducerClass(MapReduceReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		//5.Submit Job =>YARN
		boolean isSuccess = job.waitForCompletion(true);
		
		return isSuccess?0:1;
	}
	
	public static void main(String[] args) {
		try {
			int status = new MapReducePattern().run(args);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
}
