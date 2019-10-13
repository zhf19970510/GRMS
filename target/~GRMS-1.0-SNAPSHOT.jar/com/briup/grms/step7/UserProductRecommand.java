// Decompiled by Jad v1.5.8e2. Copyright 2001 Pavel Kouznetsov.
// Jad home page: http://kpdus.tripod.com/jad.html
// Decompiler options: packimports(3) fieldsfirst ansi space 
// Source File Name:   UserProductRecommand.java

package com.briup.grms.step7;

import java.io.*;
import java.sql.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.lib.db.DBWritable;

public class UserProductRecommand
	implements DBWritable, WritableComparable
{

	private Text uid;
	private Text gid;
	private IntWritable exp;

	public UserProductRecommand()
	{
		uid = new Text();
		gid = new Text();
		exp = new IntWritable();
	}

	public UserProductRecommand(String uid, String gid, int exp)
	{
		this.uid = new Text();
		this.gid = new Text();
		this.exp = new IntWritable();
		this.uid = new Text(uid);
		this.gid = new Text(gid);
		this.exp = new IntWritable(exp);
	}

	public UserProductRecommand(Text uid, Text gid, IntWritable exp)
	{
		this.uid = new Text();
		this.gid = new Text();
		this.exp = new IntWritable();
		this.uid = new Text(uid.toString());
		this.gid = new Text(gid.getBytes());
		this.exp = new IntWritable(exp.get());
	}

	public Text getUid()
	{
		return uid;
	}

	public Text getGid()
	{
		return gid;
	}

	public IntWritable getExp()
	{
		return exp;
	}

	public void setUid(Text uid)
	{
		this.uid = new Text(uid.toString());
	}

	public void setGid(Text gid)
	{
		this.gid = new Text(gid.toString());
	}

	public void setExp(IntWritable exp)
	{
		this.exp = new IntWritable(exp.get());
	}

	public int compareTo(UserProductRecommand o)
	{
		return uid.compareTo(o.uid) != 0 ? uid.compareTo(o.uid) : gid.compareTo(o.gid);
	}

	public void write(DataOutput out)
		throws IOException
	{
		uid.write(out);
		gid.write(out);
		exp.write(out);
	}

	public void readFields(DataInput in)
		throws IOException
	{
		uid.readFields(in);
		gid.readFields(in);
		exp.readFields(in);
	}

	public void write(PreparedStatement prep)
		throws SQLException
	{
		prep.setString(1, uid.toString());
		prep.setString(2, gid.toString());
		prep.setInt(3, exp.get());
	}

	public void readFields(ResultSet rs)
		throws SQLException
	{
		if (rs == null)
		{
			return;
		} else
		{
			uid = new Text(rs.getString(1));
			gid = new Text(rs.getString(2));
			exp = new IntWritable(rs.getInt(3));
			return;
		}
	}

	public String toString()
	{
		return (new StringBuilder()).append(uid.toString()).append(" ").append(gid.toString()).append(" ").append(exp.get()).toString();
	}

	public volatile int compareTo(Object obj)
	{
		return compareTo((UserProductRecommand)obj);
	}
}
