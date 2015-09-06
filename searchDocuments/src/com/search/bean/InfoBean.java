package com.search.bean;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

/**
 * 每个分词的信息
 * 
 * */
public class InfoBean implements WritableComparable<InfoBean>{

 
	private long start;//目标词汇的开始下标
	private long end;//目标词汇出现的截止下标
	private long time;//目标词汇出现的次数
	private String dscPath;//目标词汇出现的具体路径
	private String sos;//文件容器
	
	 
	public long getStart() {
		return start;
	}
	public void setStart(long start) {
		this.start = start;
	}
	public long getEnd() {
		return end;
	}
	public void setEnd(long end) {
		this.end = end;
	}

	public long getTime() {
		return time;
	}
	public void setTime(long time) {
		this.time = time;
	}
 
	
	public String getSos() {
		return sos;
	}
	public void setSos(String sos) {
		this.sos = sos;
	}
	@Override
	public String toString() {
		return  dscPath +"\t"+sos+":"+time;
	}
	public String getDscPath() {
		return dscPath;
	}
	public void setDscPath(String dscPath) {
		this.dscPath = dscPath;
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {

	 
		this.start = in.readLong();
		this.end = in.readLong();
		this.sos = in.readUTF();
		this.dscPath = in.readUTF();
		this.time = in.readLong();
	}
	@Override
	public void write(DataOutput out) throws IOException {

	 
		out.writeLong(start);
		out.writeLong(end);
		out.writeUTF(sos);
		out.writeUTF(dscPath);
		out.writeLong(time);
		
	}
	@Override
	public int compareTo(InfoBean o) {

		
		return  (int) (o.time-this.time);
	}
	
}
