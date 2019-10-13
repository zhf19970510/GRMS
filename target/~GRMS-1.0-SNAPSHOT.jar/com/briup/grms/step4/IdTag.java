// Decompiled by Jad v1.5.8e2. Copyright 2001 Pavel Kouznetsov.
// Jad home page: http://kpdus.tripod.com/jad.html
// Decompiler options: packimports(3) fieldsfirst ansi space 
// Source File Name:   IdTag.java

package com.briup.grms.step4;

import java.io.*;
import org.apache.hadoop.io.*;

public class IdTag
	implements WritableComparable
{

	private Text id;
	private IntWritable tag;

	public IdTag()
	{
		id = new Text();
		tag = new IntWritable();
	}

	public IdTag(Text id, IntWritable tag)
	{
		this.id = new Text();
		this.tag = new IntWritable();
		this.id = new Text(id.toString());
		this.tag = new IntWritable(tag.get());
	}

	public IdTag(String id, int tag)
	{
		this.id = new Text();
		this.tag = new IntWritable();
		this.id = new Text(id);
		this.tag = new IntWritable(tag);
	}

	public Text getId()
	{
		return id;
	}

	public void setId(Text id)
	{
		this.id = new Text(id.toString());
	}

	public IntWritable getTag()
	{
		return tag;
	}

	public void setTag(IntWritable tag)
	{
		this.tag = new IntWritable(tag.get());
	}

	public void readFields(DataInput in)
		throws IOException
	{
		id.readFields(in);
		tag.readFields(in);
	}

	public void write(DataOutput out)
		throws IOException
	{
		id.write(out);
		tag.write(out);
	}

	public int compareTo(IdTag o)
	{
		return id.compareTo(o.id) != 0 ? id.compareTo(o.id) : tag.compareTo(o.tag);
	}

	public volatile int compareTo(Object obj)
	{
		return compareTo((IdTag)obj);
	}
}
