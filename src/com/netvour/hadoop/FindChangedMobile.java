package com.netvour.hadoop;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 * 查找从代理商手机换卡到别的手机的手机号码
 */
public class FindChangedMobile {

	public static class MapClass extends Mapper<Object, Text, Text, NullWritable> {

		private Text mobile = new Text();
		private Set<String> imeis;

		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			super.setup(context);
			imeis = AgentImeiDao.getInstance().all();
		}
		
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			//换机的号码|号码品牌|归属地市|原IMEI号|新IEMI号
			String[] fields = value.toString().split("\\|");
			String oldImei = fields[3];
			if(imeis.contains(oldImei)) {
				mobile.set(fields[0]);
				context.write(mobile, NullWritable.get());
			}
		}
	}

	public static class Combine extends Reducer<Text, NullWritable, Text, NullWritable> {
		public void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
			context.write(key, NullWritable.get());
		}
	}
	
	public static class Reduce extends Reducer<Text, NullWritable, Text, NullWritable> {
		public void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
			context.write(key, NullWritable.get());
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: findchangedmobile <in> <out>");
			System.exit(2);
		} 
		Job job = Job.getInstance(conf, "find changed mobile");
		job.setJarByClass(FindChangedMobile.class);
		
		job.setMapperClass(MapClass.class);
		job.setCombinerClass(Combine.class);
		job.setReducerClass(Reduce.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
