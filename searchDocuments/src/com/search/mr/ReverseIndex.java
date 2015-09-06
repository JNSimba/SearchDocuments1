package com.search.mr;



import java.io.IOException;
import java.io.StringReader;
import java.util.TreeSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.wltea.analyzer.core.IKSegmenter;
import org.wltea.analyzer.core.Lexeme;

import com.search.bean.InfoBean;

/**
 * 
 * 将HDFS中的数据读取出来
 * 让MapReduce进行倒排索引处理
 * 并且产生分区表
 * 
 * */
public class ReverseIndex extends Configured implements Tool{

	public static void main(String[] args) throws Exception {
		
		ToolRunner.run(new Configuration(), new ReverseIndex(), args);
	}
	
	/**
	 * 
	 *输入：
	 * 1.txt 
	 * 		hello simba
	 * 		hello carol
	 * 2.txt
	 * 		simba and carol
	 * 
	 * 输出：
	 * hello->1.txt {(0,5),1}
	 * simba->1.txt {(6,11),1}
	 * hello->1.txt {(12,17),1}
	 * simba->2.txt {(0,5),1}
	 * 
	 * 
	 * */
	public static class IndexMap extends Mapper<LongWritable, Text, Text, InfoBean>
	{
		private InfoBean info = new InfoBean();
		private Text outKey = new Text();

		protected void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException
		{
			FileSplit is = (FileSplit)context.getInputSplit();
			Path path = is.getPath();
			String fileName = path.getName();
			
			StringReader sr = new StringReader(value.toString());
			IKSegmenter iks = new IKSegmenter(sr,false);
			
			Lexeme le = null;
			while((le=iks.next())!=null)
			{
				String text = le.getLexemeText();
				long start = le.getBeginPosition();
				long end = le.getEndPosition();
				
			 
				info.setStart(start);
				info.setEnd(end);
				info.setTime(1);
				info.setSos(" ");
				info.setDscPath(" ");
			//	System.out.println(info.toString()+".......");
				outKey.set(text+"->"+fileName);
				context.write(outKey,info);
			}
			
		}
		
	}
	/**
	 *输入：
	 * hello->1.txt {(0,5),1};{(12,17),1}
	 * simba->1.txt {(6,11),1}
	 *
	 * simba->2.txt {(0,5),1}
	 * 
	 * 输出：
	 * 
	 * simba  1.txt:{(6,11):1}
	 * simba  2.txt:{(0,5):1}
	 * hello  1.txt:{(0,5),(12,17):2}
	 * */
	public static class IndexCombine extends Reducer<Text, InfoBean, Text, InfoBean>
	{

		private Text kt = new Text();
		@Override
		protected void reduce(Text key, Iterable<InfoBean> values,Context context)
				throws IOException, InterruptedException {
		 
			long sum = 0;//在每个文件中出现的次数
			StringBuilder sb = new StringBuilder();
			
			String temp =null  ;//临时存放
			sb.append("{");
			for(InfoBean info : values)
			{
				sum+=info.getTime();
				temp = "("+info.getStart()+","+info.getEnd()+") ";
				sb.append(temp) ;
			}
			sb.append("}");
			
			InfoBean newInfo = new InfoBean();
			newInfo.setTime(sum);
			newInfo.setSos(sb.toString());
			
			String []field = key.toString().split("->");
			String fileName = field[1];
			newInfo.setDscPath(fileName);
	//		newInfo.setText(field[0]);
	
			kt.set(field[0]);//设置单词
			
			context.write(kt, newInfo);
			
			
		}
		
	}
	/**
	 * 输入：
	 * simba  1.txt:{(6,11):1}; 2.txt:{(0,5):1}
	 * hello  1.txt:{(0,5),(12,17):2}
	 * 
	 * 输出：
	 * 
	 * simba   1.txt:{(6,11):1};2.txt:{(0,5):1}//让按照每篇文章出现的最多次数从高到低排序
	 * 
	 * */
	public static class IndexReduce extends Reducer<Text, InfoBean,Text,Text>
	{
		private Text v = new Text();
		protected void reduce(Text key , Iterable<InfoBean> values ,Context context)throws IOException, InterruptedException 
		{
		
			TreeSet<InfoBean> treeInfo = new TreeSet<InfoBean>();

			for(InfoBean in : values)
			{
				InfoBean i = new InfoBean();
				i.setDscPath(in.getDscPath());
				i.setTime(in.getTime());
				i.setSos(in.getSos());
				treeInfo.add(i);
			}
			
				v.set(treeInfo.toString());
				context.write(key,v);
			
		}
		 
	}
 
	@Override
	public int run(String[] args) throws Exception {
		
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		
		job.setJarByClass(ReverseIndex.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(InfoBean.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(InfoBean.class);
		
		job.setCombinerClass(IndexCombine.class);
		job.setMapperClass(IndexMap.class);
		job.setReducerClass(IndexReduce.class);
		
		Path[] path = new Path[args.length-1];
		for(int i=1;i<args.length;i++)
			path[i-1] = new Path(args[i]);
		
		FileInputFormat.setInputPaths(job, path);
		FileOutputFormat.setOutputPath(job, new Path(args[0]));
		
		return job.waitForCompletion(true)? 0 : 1;
	}
}

