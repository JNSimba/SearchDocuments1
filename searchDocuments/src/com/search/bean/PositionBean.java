package com.search.bean;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;


/**
 * 
 * 二级索引表的输出项
 * 
 * */
public class PositionBean implements Writable{

	private String tableName;
	private long offset;
	
	public String getTableName() {
		return tableName;
	}
	public void setTableName(String tableName) {
		this.tableName = tableName;
	}
	public long getOffset() {
		return offset;
	}
	public void setOffset(long offset) {
		this.offset = offset;
	}
	@Override
	public String toString() {
		return  tableName + " " + offset;
	}
	@Override
	public void write(DataOutput out) throws IOException {
		 
		out.writeUTF(tableName);
		out.writeLong(offset);
	}
	@Override
	public void readFields(DataInput in) throws IOException {
		 
		this.tableName = in.readUTF();
		this.offset = in.readLong();
		
	}
 
}
