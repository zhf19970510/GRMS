// Decompiled by Jad v1.5.8e2. Copyright 2001 Pavel Kouznetsov.
// Jad home page: http://kpdus.tripod.com/jad.html
// Decompiler options: packimports(3) fieldsfirst ansi space 
// Source File Name:   PreResult.java

package com.briup.grms.step6;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class PreResult extends Configured
	implements Tool
{
	public static class PRMapper extends Mapper
	{

		protected void map(LongWritable key, Text value, org.apache.hadoop.mapreduce.Mapper.Context context)
			throws IOException, InterruptedException
		{
			String s1 = value.toString();
			String strs[] = s1.split(" ");
			context.write(new Text((new StringBuilder()).append(strs[1]).append(",").append(strs[0]).toString()), new IntWritable(1));
		}

		protected volatile void map(Object obj, Object obj1, org.apache.hadoop.mapreduce.Mapper.Context context)
			throws IOException, InterruptedException
		{
			map((LongWritable)obj, (Text)obj1, context);
		}

		public PRMapper()
		{
		}
	}


	public PreResult()
	{
	}

	public static void main(String args[])
		throws Exception
	{
		ToolRunner.run(new PreResult(), args);
	}

	public int run(String strings[])
		throws Exception
	{
		Configuration conf = getConf();
		Job job = Job.getInstance(conf, "zenghongfa_PreResult");
		job.setJarByClass(getClass());
		Path inpath = new Path(conf.get("inpath"));
		Path outpath = new Path(conf.get("outpath"));
		job.setInputFormatClass(org/apache/hadoop/mapreduce/lib/input/TextInputFormat);
		job.setOutputFormatClass(org/apache/hadoop/mapreduce/lib/output/TextOutputFormat);
		TextInputFormat.addInputPath(job, inpath);
		TextOutputFormat.setOutputPath(job, outpath);
		job.setMapperClass(com/briup/grms/step6/PreResult$PRMapper);
		job.setMapOutputKeyClass(org/apache/hadoop/io/Text);
		job.setMapOutputValueClass(org/apache/hadoop/io/IntWritable);
		job.setReducerClass(org/apache/hadoop/mapreduce/Reducer);
		job.setOutputKeyClass(org/apache/hadoop/io/Text);
		job.setOutputValueClass(org/apache/hadoop/io/IntWritable);
		return job.waitForCompletion(true) ? 0 : 1;
	}
}
