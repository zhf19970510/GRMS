// Decompiled by Jad v1.5.8e2. Copyright 2001 Pavel Kouznetsov.
// Jad home page: http://kpdus.tripod.com/jad.html
// Decompiler options: packimports(3) fieldsfirst ansi space 
// Source File Name:   GoodsCoocurrenceList.java

package com.briup.grms.step2;

import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class GoodsCoocurrenceList extends Configured
	implements Tool
{
	public static class GCLReducer extends Reducer
	{

		protected void reduce(Text key, Iterable values, org.apache.hadoop.mapreduce.Reducer.Context context)
			throws IOException, InterruptedException
		{
			int sum = 0;
			for (Iterator iterator = values.iterator(); iterator.hasNext();)
			{
				IntWritable i = (IntWritable)iterator.next();
				sum += i.get();
			}

			context.write(key, new IntWritable(sum));
		}

		protected volatile void reduce(Object obj, Iterable iterable, org.apache.hadoop.mapreduce.Reducer.Context context)
			throws IOException, InterruptedException
		{
			reduce((Text)obj, iterable, context);
		}

		public GCLReducer()
		{
		}
	}

	public static class GCLMappper extends Mapper
	{

		protected void map(LongWritable key, Text value, org.apache.hadoop.mapreduce.Mapper.Context context)
			throws IOException, InterruptedException
		{
			String values[] = value.toString().split("\t");
			String datas[] = values[1].split(",");
			for (int i = 0; i < datas.length; i++)
			{
				for (int j = 0; j < datas.length; j++)
				{
					String pair = null;
					pair = (new StringBuilder()).append(datas[i]).append(",").append(datas[j]).toString();
					context.write(new Text(pair), new IntWritable(1));
				}

			}

		}

		protected volatile void map(Object obj, Object obj1, org.apache.hadoop.mapreduce.Mapper.Context context)
			throws IOException, InterruptedException
		{
			map((LongWritable)obj, (Text)obj1, context);
		}

		public GCLMappper()
		{
		}
	}


	public GoodsCoocurrenceList()
	{
	}

	public static void main(String args[])
		throws Exception
	{
		ToolRunner.run(new GoodsCoocurrenceList(), args);
	}

	public int run(String strings[])
		throws Exception
	{
		Configuration conf = getConf();
		Job job = Job.getInstance(conf, "zhf_GoodsCoocurrenceList");
		job.setJarByClass(getClass());
		job.setMapperClass(com/briup/grms/step2/GoodsCoocurrenceList$GCLMappper);
		job.setMapOutputKeyClass(org/apache/hadoop/io/Text);
		job.setMapOutputValueClass(org/apache/hadoop/io/IntWritable);
		job.setReducerClass(com/briup/grms/step2/GoodsCoocurrenceList$GCLReducer);
		job.setOutputKeyClass(org/apache/hadoop/io/Text);
		job.setOutputValueClass(org/apache/hadoop/io/IntWritable);
		job.setInputFormatClass(org/apache/hadoop/mapreduce/lib/input/TextInputFormat);
		TextInputFormat.addInputPath(job, new Path(conf.get("inpath")));
		job.setOutputFormatClass(org/apache/hadoop/mapreduce/lib/output/TextOutputFormat);
		TextOutputFormat.setOutputPath(job, new Path(conf.get("outpath")));
		return job.waitForCompletion(true) ? 0 : 1;
	}
}
