package com.search.main;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Calendar;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ToolRunner;

import com.search.mr.ReverseIndex;
import com.search.mr.TokenIndex;


/**
 * 
 * 所有程序的主入口
 * 首先检查是否有更新，然后在判断是否需要重建倒排索引 
 * 
 * */
public class Main {
	
	private static String hdfsfile = "/search/data/";
	/**
	 * 参数：
	 * 源文件倒排索引后的目录；源文件建立二级索引后的目录，源文件.....；
	 * 
	 * */
	public static void main(String[] args) throws Exception {
		
		
		if(!checkUpdate())
		{
			System.out.println("没有新更新的文件，不用进行处理");
			return ;
		}
		
		
		Configuration conf = new Configuration();
		String [] data =  new String[args.length-1];
		data[0] = args[0];//源文件倒排索引
		
		for(int i=2;i<args.length;i++)
			data[i-1] = args[i];		//源文件

		if(ToolRunner.run(conf,new ReverseIndex(), data) == 0)
		{
			String[] index = new String[2];
			
			index[0] = data[0]+"/*";  //所有的倒排索引文件放置处
			index[1] = args[1];  //二级索引放置处
			ToolRunner.run(conf,new TokenIndex(), index);
		}
		
	}

	private static boolean checkUpdate() throws IOException, InterruptedException, URISyntaxException {
		 
		FileSystem fs = FileSystem.get(new URI("hdfs://192.168.1.88:9000"), new Configuration(), "root");
		Path pathDir = new Path(hdfsfile);
		FileStatus[] listStatus = fs.listStatus(pathDir);
	 
		Calendar currcal = Calendar.getInstance();
		currcal.add(Calendar.DATE, -1);
		
		boolean isUpdate = false;
		for(FileStatus fstemp : listStatus)
		{
			 Calendar caltemp = Calendar.getInstance();
			 caltemp.setTimeInMillis(fstemp.getModificationTime());
			 if(caltemp.after(currcal))
			 {
				 System.out.println(fstemp.getPath());//新更新的文件
				 isUpdate = true;
				 break;
			 }
		}
		
	//	System.out.println(isUpdate);
		return isUpdate;
	}

	
}
