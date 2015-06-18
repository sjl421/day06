package org.preprocess;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.util.AnalysisNginxTool;
import org.util.DateToNUM;

public class AccessLogPreProcess {
	public static class AccessLogPreProcessMapper extends
			Mapper<Object, Text, Text, NullWritable> {
		private Text word = new Text();
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			String itr[] = value.toString().split(" ");
			if (itr.length < 11) {
				return;
			}
			String ip = itr[0];
			String date = AnalysisNginxTool.nginxDateStmpToDate(itr[3]);
			String url = itr[6];
			String urlref = itr[10];
			word.set(ip + "," + date + "," + url + "," + urlref);
			context.write(word, NullWritable.get());
		}
	}

	public static class AccessLogPreProcessReducer extends
			Reducer<Text, NullWritable, Text, NullWritable> {
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			context.write(key, NullWritable.get());
		}
	}

	public static void main(String[] args) throws Exception {
		DateToNUM.initMap();
		Configuration conf = new Configuration();
		String[] otherArgs = { "hdfs://master:9000/access.log",
				"hdfs://master:9000/uvout" };
		Job job = new Job(conf, "preprocess"); // 设置一个用户定义的job名称
		job.setJarByClass(AccessLogPreProcess.class);
		job.setMapperClass(AccessLogPreProcessMapper.class); // 为job设置Mapper类
		job.setReducerClass(AccessLogPreProcessReducer.class); // 为job设置Reducer类
		job.setOutputKeyClass(Text.class); // 为job的输出数据设置Key类
		job.setOutputValueClass(NullWritable.class); // 为job输出设置value类
		FileInputFormat.addInputPath(job, new Path(otherArgs[0])); // 为job设置输入路径
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));// 为job设置输出路径
		System.exit(job.waitForCompletion(true) ? 0 : 1); // 运行job
	}
}
