package com.search.mr;
  
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

/**
 * 将倒排索引的结果存入HBase
 * 
 * 主键是单词
 * 列族是detail，列分别为text、pos、time（文件名称、位置、出现次数）
 * 
 * 不同的文件分成不同的版本存储
 * 
 * */
public class SearchHBase {
	private static String hdfsurl = "/search/index/*";
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException
	{
		Configuration conf = new Configuration();
		conf.set("hbase.zookeeper.quorum", "192.168.1.88");
		conf.set(TableOutputFormat.OUTPUT_TABLE,"infotable");//192.168.1.88上的HBase中
		conf.set("dfs.socket.timeout","180000");
		
		Job job = Job.getInstance(conf);
		job.setJarByClass(SearchHBase.class);
		
		job.setMapperClass(IndexMap.class);
		job.setReducerClass(IndexReduce.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setOutputFormatClass(TableOutputFormat.class);
		FileInputFormat.setInputPaths(job, hdfsurl);
		
		job.waitForCompletion(true);
		
	}
	/*
	 * 与	[big	{(17,18) (21,22) (173,174) }:3 , small	{(56,57) }:1 ]
	 * */
	public static class IndexMap extends Mapper<LongWritable,Text,Text,Text>
	{
		private Text outKey = new Text();
		private Text outValue = new Text();
		public  StringBuilder sb = new StringBuilder();
		protected void map(LongWritable key,Text value,Context context)throws IOException,InterruptedException
		{
			
			String[] field = value.toString().split(", ");
			
			String word = field[0].split("\t")[0];
			
		 	String text = field[0].substring(field[0].indexOf("[")+1,field[0].lastIndexOf("\t"));
			String path =  field[0].substring(field[0].lastIndexOf("\t")+1,field[0].indexOf(":"));
			String time = field[0].substring(field[0].indexOf(":")+1,field[0].length()-1);
		 
			outKey.set(word);
			
		 	String record = text+":"+path+":"+time;
		 	sb.append(record+";");
		 	int index = 1;
			while(index < field.length)
			{
				String t = field[index].substring(0, field[index].indexOf("\t"));
				String p = field[index].substring(field[index].indexOf("\t")+1,field[index].indexOf(":"));
				String ti = field[index].substring(field[index].indexOf(":")+1,field[index].length()-2);
				
				String temRecord = t+":"+p+":"+ti;
				sb.append(temRecord+";");
				++index;
			}
			 
			outValue.set(sb.toString());
			
			context.write(outKey, outValue);
			
		}
	}
	
	public static class IndexReduce extends TableReducer<Text,Text,NullWritable>
	{
		protected void reduce(Text key,Iterable<Text> values,Context context)throws IOException,InterruptedException
		{
		 	 
			for(Text v : values)
			{ 
				String[] record = v.toString().split(";");
				int index =0;
				Put put = new Put(Bytes.toBytes(key.toString()));
				
				while(index < record.length)
				{
					String [] temp = record[index].split(":");
					put.add(Bytes.toBytes("detail"),Bytes.toBytes("text"),Bytes.toBytes(temp[0]));
					put.add(Bytes.toBytes("detail"),Bytes.toBytes("pos"),Bytes.toBytes(temp[1]));
					put.add(Bytes.toBytes("detail"),Bytes.toBytes("time"),Bytes.toBytes(temp[2]));
					
					context.write(NullWritable.get(),put);
					++index;
				}
			 	
			} 
			
		}
	}
}

