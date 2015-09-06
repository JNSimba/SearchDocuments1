package com.search.mr;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.search.bean.PositionBean;


/**
 * 
 * 建立二级索引
 * 输入是所有的倒排索引文件
 * 输出是：
 * word		所在的索引文件名+偏移量
 * 
 * 
 * */
public class TokenIndex extends Configured implements Tool{

	public static void main(String[] args) throws Exception {
		
		ToolRunner.run(new Configuration(), new TokenIndex(), args);
		
	}
	
	public static class TokenIndexMap extends Mapper<LongWritable, Text, Text, Text>
	{
		private Text t = new Text();
		private Text v = new Text();
		protected void map(LongWritable key ,Text value,Context context) throws IOException, InterruptedException
		{
			String word = value.toString().split("\t")[0];
			FileSplit inputSplit = (FileSplit)context.getInputSplit();
			Path path = inputSplit.getPath();
			String name = path.getName();
			long offset = key.get();
			PositionBean po = new PositionBean();
			po.setTableName(name);
			po.setOffset(offset);
			
			t.set(word);
			v.set(po.toString());
			context.write(t,v);
		}
	}

	@Override
	public int run(String[] args) throws Exception {

		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		job.setJarByClass(TokenIndex.class);
		
		job.setMapperClass(TokenIndexMap.class);
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(PositionBean.class);
		
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		return job.waitForCompletion(true) ? 0 : 1;
	}
}
