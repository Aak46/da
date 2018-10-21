package firstreduce;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.StringTokenizer;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
//import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.metrics2.impl.ConfigBuilder;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.StringUtils;

import com.sun.org.apache.xml.internal.serialize.OutputFormat;

public class Wordcnt {

	public static class Map extends Mapper<LongWritable,Text,Text,IntWritable> {
		public void map(LongWritable key,Text value,Context context)
		throws IOException,InterruptedException {
			
			String line = value.toString();     //convert value as string like value= I am aadesh
			
			StringTokenizer tokenizer = new StringTokenizer(line);   //removes blank spaces and find words
			
			while (tokenizer.hasMoreTokens()) {
				value.set(tokenizer.nextToken());
				context.write(value, new IntWritable(1));
			}
			
		}
	}
	
	public static class Reduce extends Reducer<Text,IntWritable,Text,IntWritable>{
		
		public void reduce(Text key,Iterable<IntWritable> values, Context context)
		throws IOException ,InterruptedException {
			int sum=0;
			
			for(IntWritable x:values)
			{
				sum+=x.get();
			}
			
			context.write(key, new IntWritable(sum));
		}
		
	}
	
	public static void main (String[] args) throws Exception {
		
		Configuration conf = new Configuration();
		Job job=Job.getInstance(conf,"Wordcnt");
		
		job.setJarByClass(Wordcnt.class);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
//		Job.setOutputFormatClass(TextOutputFormat<? extends TextOutputFormat>);
		
		Path outputPath = new Path(args[1]);
		
		//configuring the input/output path from the file system  into the job
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		//hadoop jar wordcountjar
		
		//deleting o/p pathautomatically from hdfs so we dont have to delete manually
		outputPath.getFileSystem(conf).delete(outputPath,true);
		
		//exiting the job if flag value beccmes false
		System.exit(job.waitForCompletion(true)?0 :1);
		
		
		
		
	}
}
