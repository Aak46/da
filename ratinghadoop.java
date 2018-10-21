package soundTrackAnalysis;
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
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.metrics2.impl.ConfigBuilder;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.StringUtils;

import com.sun.org.apache.xml.internal.serialize.OutputFormat;


public class Soundclass {
	
	public class LastFMConstants{
		
		public static final int user_Id = 0;
		public static final int track_Id=1;
		public static final int shared = 2;
		public static final int radio =3;
		public static final int skipped = 4;
	}
	
	public static class UniqueListenerMapper extends Mapper<Object, Text, IntWritable, IntWritable> {
		
		IntWritable trackid = new IntWritable();
		IntWritable userid = new IntWritable();
		
		public void map(Object key,Text value, Mapper<Object,Text,IntWritable,IntWritable>.Context context)
		throws IOException,InterruptedException {
			
			String[] parts = value.toString().split("[|]");
			trackid.set(Integer.parseInt(parts[LastFMConstants.track_Id]));
			userid.set(Integer.parseInt(parts[LastFMConstants.user_Id]));
			
			context.write(trackid, userid);
		}
	}
	
	public static class UniqueListenerReducer extends Reducer<IntWritable,IntWritable,IntWritable,IntWritable> {
		
		public void reduce (
			IntWritable trackid,
			Iterable<IntWritable> userIds ,
			Reducer<IntWritable,IntWritable,IntWritable,IntWritable>.Context context)
			throws IOException,InterruptedException {
			
			Set<Integer> userIdSet = new HashSet<Integer>();
			
			for (IntWritable userid : userIds) {
				userIdSet.add(userid.get());
				
			}
			
			IntWritable size = new IntWritable(userIdSet.size());
			context.write(trackid, size);
				
			}
		}
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		if (args.length != 2) {
			System.err.println("Usage: uniquelisteners <in> <out>");
			System.exit(2);
		}
		Job job = new Job(conf, "Unique listeners per track");
		job.setJarByClass(Soundclass.class);
		job.setMapperClass(UniqueListenerMapper .class);
		job.setReducerClass(UniqueListenerReducer.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
		org.apache.hadoop.mapreduce.Counters counters = job.getCounters();
		
}
	
}
	
	
	






