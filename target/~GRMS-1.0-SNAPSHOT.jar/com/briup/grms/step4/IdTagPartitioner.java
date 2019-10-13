// Decompiled by Jad v1.5.8e2. Copyright 2001 Pavel Kouznetsov.
// Jad home page: http://kpdus.tripod.com/jad.html
// Decompiler options: packimports(3) fieldsfirst ansi space 
// Source File Name:   IdTagPartitioner.java

package com.briup.grms.step4;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

// Referenced classes of package com.briup.grms.step4:
//			IdTag

public class IdTagPartitioner extends Partitioner
{

	public IdTagPartitioner()
	{
	}

	public int getPartition(IdTag key, Text value, int num)
	{
		String str = key.getId().toString();
		return Math.abs(str.hashCode() % num);
	}

	public volatile int getPartition(Object obj, Object obj1, int i)
	{
		return getPartition((IdTag)obj, (Text)obj1, i);
	}
}
