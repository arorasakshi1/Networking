package demo.datee;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Date;
import java.util.HashMap;


import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.Mapper;

import demo.datee.eb.month;
 @SuppressWarnings("deprecation")
public class counter_mapper extends Mapper<LongWritable, Text, Text , Text>{
	
	 protected Text out= new Text();
	  public void map ( LongWritable key, Text value,
              Context context) throws IOException, InterruptedException
 {
		  
		  String line=value.toString();
		  String[] strs=line.split(",");
		  long lts=Long.parseLong(strs[1]);
		  Date time=new Date(lts);
		  int m=time.getMonth();
		  if(m==11)
		  {
			  context.getCounter(month.Dec).increment(10);
		  }
		  if(m==0)
		  {
			  context.getCounter(month.jan).increment(20);
		  }
		  if (m==1)
		  {
			  context.getCounter(month.feb).increment(20);
		  }
		  out.set("success");
	 context.write(out, out);
 }
 
}

