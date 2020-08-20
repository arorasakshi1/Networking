package demo.datee;
import java.io.IOException;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
//import org.apache.hadoop.mapred.FileInputFormat.Counter;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.CounterGroup;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import demo.datee.eb.month;

public class driver extends Configured implements Tool{
	
	public static void main(String[] args) throws Exception{
		int exitCode = ToolRunner.run(new driver(), args);
		System.exit(exitCode);
	}
 
	public int run(String[] args) throws Exception {
		if (args.length != 2) {
			System.err.printf("Usage: %s [generic options] <input> <output>\n",
					getClass().getSimpleName());
			ToolRunner.printGenericCommandUsage(System.err);
			return -1;
		}
	
		@SuppressWarnings("deprecation")
		Job job = new org.apache.hadoop.mapreduce.Job();
		job.setJarByClass(driver.class);
		job.setJobName("WordCounter");
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
	
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setMapperClass(counter_mapper.class);
		job.setNumReduceTasks(0);
	
		int returnValue = job.waitForCompletion(true) ? 0:1;
		System.out.println("job.isSuccessful " + job.isSuccessful());
		


						Counters cn=job.getCounters();
						// Find the specific counters that you want to print
						Counter c1=cn.findCounter(month.Dec);
						System.out.println(c1.getDisplayName()+":"+c1.getValue());
						Counter c2=cn.findCounter(month.jan);
						System.out.println(c2.getDisplayName()+":"+c2.getValue());
						Counter c3=cn.findCounter(month.feb);
						System.out.println(c2.getDisplayName()+":"+c3.getValue());
						/* We can get all the available counters from CounterGroup instance and print them all in loop*/
						for (CounterGroup group : cn) {
						System.out.println("* Counter Group: " + group.getDisplayName() + " (" + group.getName() + ")");
						System.out.println(" number of counters in this group: " + group.size());
						for (Counter counter : group) {
						System.out.println(" - " + counter.getDisplayName() + ": " + counter.getName() + ": "+counter.getValue());
						}
						}
						return returnValue;
	}
}
	