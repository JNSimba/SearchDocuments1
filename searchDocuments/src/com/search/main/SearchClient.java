package com.search.main;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.StringReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IOUtils;
import org.wltea.analyzer.core.IKSegmenter;
import org.wltea.analyzer.core.Lexeme;


/**
 * 文档检索系统
 * 倒排表分区成5个，然后针对这5个分区，建立二级索引表
 * 
 * 该类是用来查询某个单词出现在文档中出处，模拟客户端，给前台提供接口
 * 
 * 
 * */

public class SearchClient {

	private static String hdfsurl ="hdfs://192.168.1.88:9000"; 
	private static Configuration conf = new Configuration();
	private static HashMap<String,String> map = new HashMap<String,String>();
	private static String fileDir = "/search/data/";
	private static String secondIndexFile = "/search/secondaryIndex/part-r-00000";
	private static String indexDir = "/search/index/";
	
	
	public static void main(String[] args) throws Exception
	{
		String str = "地理信息系统计算机学院";
		
		List<String> result = queryWord(str);
		System.out.println(result);
		
		List<String> resu = queryWordForHBase(str);
		System.out.println(resu);
		
		queryWordForHBase(str,"text");
		
	}
	public static List<String> queryWordForHBase(String str,String type) throws IOException
	{
		List<String> relist = null;
		if(type.equals("text")||type.equals("pos")||type.equals("time"))
			relist  = queryHBaseforType(str,type);
		else 
			throw new RuntimeException("the type is wrong");
		
		return relist;
	}
	
	private static List<String> queryHBaseforType(String str,String type) throws IOException {
	 
		HTable ht = new HTable(conf,"infotable");
		List<String> arr  = transWord(str);//分词
		List<String> resultList = new ArrayList<String>();
		
		for(String s : arr)
		{
			Get get = new Get(Bytes.toBytes(s));
		 	get.addColumn(Bytes.toBytes("detail"), Bytes.toBytes(type));
	 
			Result  result =ht.get(get);
		 
			byte[] temp = result.getValue(Bytes.toBytes("detail"), Bytes.toBytes(type));

			if(temp==null)
			   	continue;
			 
		    byte[] name = result.getRow();
			String sn = new String(name,"UTF-8");
		    
			String stemp = new String(temp,"UTF-8");

			StringBuilder sb = new StringBuilder();
			sb.append(sn).append(":").append(stemp);
			
			resultList.add(sb.toString());
		}
		
		ht.close();
		 
		
		return resultList;
	}
	private static List<String> queryWordForHBase(String str) throws IOException
	{
		HTable ht = new HTable(conf,"infotable");
		 
		List<String> arr  = transWord(str);//分词
		List<String> resultList = new ArrayList<String>();

		for(String s : arr)
		{
			Get get = new Get(Bytes.toBytes(s));
			get.addFamily(Bytes.toBytes("detail"));
		
			Result  result =ht.get(get);
			
			byte[] pos = result.getValue(Bytes.toBytes("detail"), Bytes.toBytes("pos"));
			byte[] text = result.getValue(Bytes.toBytes("detail"), Bytes.toBytes("text"));
			byte[] time = result.getValue(Bytes.toBytes("detail"), Bytes.toBytes("time"));
		    if(pos==null || text==null || time==null)
			   	continue;
			 
		    byte[] name = result.getRow();
			String sn = new String(name,"UTF-8");
		    
			String stext = new String(text,"UTF-8");
			String spos = new String(pos,"UTF-8");
			String stime = new String(time,"UTF-8");
			
			StringBuilder sb = new StringBuilder();
			sb.append(sn).append(":").append(stext).append(":").append(spos).append(":").append(stime);
			
			resultList.add(sb.toString());
		}
		
		ht.close();
		return resultList;
	}
	 

	public static void uploadFile(File file) throws IOException, InterruptedException, URISyntaxException
	{
		if(!file.isFile())
			throw new FileNotFoundException();
		
		BufferedInputStream buis = new BufferedInputStream(new FileInputStream(file));
		FileSystem fs = FileSystem.get(new URI(hdfsurl+fileDir),conf,"root");
		OutputStream out = fs.create(new Path(file.getName()));
		
		IOUtils.copyBytes(buis, out, 4096, true);	 	
	}
		
	/**
	 * 查询对应的单词的出处
	 * 
	 * */
	public static List<String> queryWord(String str)throws Exception
	{
		List<String> arr  = transWord(str);//分词
		readHdfs(arr);	//将索引表读入内存
		List<String> pos = searchInfo(arr);//读取包含的  原表+偏移
		return searchSrc(pos);//在原表中查询，单词的具体信息
	}
	
	/**
	 * 在对应的表中直接定位到相关偏移量，直接进行搜索
	 * 
	 * 
	 * */
	private static List<String> searchSrc(List<String> pos) throws Exception
	{
		FileSystem fs = FileSystem.get(new URI(hdfsurl),conf); 
		List<String> result = new ArrayList<String>();
		
		for(String st : pos)
		{
			String [] infoOfWord = st.split(" ");
			String textInfo = infoOfWord[0];
			int textPos = Integer.parseInt(infoOfWord[1]);
			String url = indexDir+textInfo;
			Path pa = new Path(url);
			if(fs.exists(pa))
			{
				FSDataInputStream fsds = fs.open(new Path(url));
				fsds.seek(textPos);
				BufferedReader bufr = new BufferedReader(new InputStreamReader(fsds));
				String line = bufr.readLine();		
				result.add(line);
			}
		 }
		return result;	
	}

	/**
	 * 将分好的词语在二级索引表中查询，获得对应的表及偏移量
	 * 以list形式返回
	 * */
	private static List<String> searchInfo(List<String> arr) {
		
		List<String> pos = new ArrayList<String>();
		for(String ss : arr)
		{
			if(map.containsKey(ss))
			{
				pos.add(map.get(ss));
			}
		}
		return pos;
	}

	/**
	 * 将整理好的二级索引文件读入HashMap中
	 * 
	 * */
	private static void readHdfs(List<String> str) throws Exception
	{
		
		FileSystem fs = FileSystem.get(new URI(hdfsurl),conf);
		FSDataInputStream fsis = fs.open(new Path(secondIndexFile));
		BufferedReader buf = new BufferedReader(new InputStreamReader(fsis));
		
		String line = null;
		while((line = buf.readLine())!=null)
		{
			String[] field = line.split("\t");
			map.put(field[0],field[1]);
		}
	}
	/**
	 * 将用户传进来的词语分成若干个
	 * 返回list集合
	 * */
	private static List<String> transWord(String word) throws IOException {

		StringReader sr = new StringReader(word);
		IKSegmenter ik = new IKSegmenter(sr,false);
		
		List<String> li = new ArrayList<String>();
		Lexeme le = null;
		
		while((le = ik.next())!=null)
		{
			String key = le.getLexemeText();
			 li.add(key);
		}
		
		return li;
	}
}

