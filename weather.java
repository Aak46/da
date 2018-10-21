package weatherdataanalysis;
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

import firstreduce.Wordcnt;
import firstreduce.Wordcnt.Map;
import firstreduce.Wordcnt.Reduce;

public class weatherdataanalysis {
	
	public static class MaxMinTemprature {
		
		public static class MaxMinTempratureMapper extends Mapper<LongWritable, Text, Text , Text> {
			
			public static final int MISSING = 9999;
			
			@Override
			public void map(LongWritable arg0,Text value,Context context)throws IOException,InterruptedException {
				
				String line = value.toString();
				
				//check if limit not empty or line is not blanked
				if(!(line.length() ==0)) {
					//fetching date (characterwise)
					String date = line.substring(6,14);
					
					//fetching maximum temprature
					float temp_max = Float
							.parseFloat(line.substring(39,45).trim());
					
					//fetching minimum temprature
					float temp_min = Float.parseFloat(line.substring(47,53).trim());
					
					//if maximum temprature is greater than 35,its hot day
					if(temp_max>35.0 && temp_max !=MISSING) {
						context.write(new Text("HOT day"+date),
								new Text(String.valueOf(temp_max)));
						
					}
					
					if(temp_min < 10 && temp_min != MISSING) {
						context.write(new Text("cold day"+date),
								new Text(String.valueOf(temp_min)));
					}
					
					
					
				}
			}
		}
		
		public static class MaxMinTempratureReducer extends Reducer<Text,Text,Text,Text>{
		
	}

		
		public static void main(String[] args) throws Exception {
			
			Configuration conf = new Configuration();
			Job job=Job.getInstance(conf,"weather example");
			
			
			job.setJarByClass(weatherdataanalysis.class);
			
			job.setMapOutputKeyClass(Text.class);
			
			job.setMapOutputValueClass(Text.class);
			
			job.setMapperClass(MaxMinTempratureMapper.class);
			
			job.setReducerClass(MaxMinTempratureReducer.class);
			
			job.setInputFormatClass(TextInputFormat.class);
			
			job.setOutputFormatClass(TextOutputFormat.class);
			
			Path OutputPath = new Path(args[1]);
			
			FileInputFormat.addInputPath(job, new Path(args[0]));
			
			FileOutputFormat.setOutputPath(job, new Path(args[1]));
			
			OutputPath.getFileSystem(conf).delete(OutputPath,true);
			
			System.exit(job.waitForCompletion(true)? 0 :1);
			
		}
}
}
