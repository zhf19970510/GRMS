// Decompiled by Jad v1.5.8e2. Copyright 2001 Pavel Kouznetsov.
// Jad home page: http://kpdus.tripod.com/jad.html
// Decompiler options: packimports(3) fieldsfirst ansi space 
// Source File Name:   GetCurrent.java

package com.briup.grms.step2;

import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class GetCurrent extends Configured
	implements Tool
{
	public static class GCReducer extends Reducer
	{

		protected void reduce(Text key, Iterable values, org.apache.hadoop.mapreduce.Reducer.Context context)
			throws IOException, InterruptedException
		{
			String str = values.toString();
			StringBuffer sb = new StringBuffer();
			Text value;
			for (Iterator iterator = values.iterator(); iterator.hasNext(); sb.append(value).append(" "))
				value = (Text)iterator.next();

			context.write(key, new Text(sb.toString()));
		}

		protected volatile void reduce(Object obj, Iterable iterable, org.apache.hadoop.mapreduce.Reducer.Context context)
			throws IOException, InterruptedException
		{
			reduce((Text)obj, iterable, context);
		}

		public GCReducer()
		{
		}
	}

	public static class GCMapper extends Mapper
	{

		protected void map(LongWritable key, Text value, org.apache.hadoop.mapreduce.Mapper.Context context)
			throws IOException, InterruptedException
		{
			String str = value.toString();
			String strs[] = str.split(",");
			String values[] = strs[1].split("\t");
			String s = (new StringBuilder()).append(values[0]).append(":").append(values[1]).toString();
			context.write(new Text(strs[0]), new Text(s));
		}

		protected volatile void map(Object obj, Object obj1, org.apache.hadoop.mapreduce.Mapper.Context context)
			throws IOException, InterruptedException
		{
			map((LongWritable)obj, (Text)obj1, context);
		}

		public GCMapper()
		{
		}
	}


	public GetCurrent()
	{
	}

	public static void main(String args[])
		throws Exception
	{
		ToolRunner.run(new GetCurrent(), args);
	}

	public int run(String strings[])
		throws Exception
	{
		Configuration conf = getConf();
		Job job = Job.getInstance(conf, "zhf_GetCurrent");
		job.setJarByClass(getClass());
		job.setMapperClass(com/briup/grms/step2/GetCurrent$GCMapper);
		job.setMapOutputKeyClass(org/apache/hadoop/io/Text);
		job.setMapOutputValueClass(org/apache/hadoop/io/Text);
		job.setReducerClass(com/briup/grms/step2/GetCurrent$GCReducer);
		job.setOutputKeyClass(org/apache/hadoop/io/Text);
		job.setOutputValueClass(org/apache/hadoop/io/Text);
		job.setInputFormatClass(org/apache/hadoop/mapreduce/lib/input/TextInputFormat);
		TextInputFormat.addInputPath(job, new Path(conf.get("inpath")));
		job.setOutputFormatClass(org/apache/hadoop/mapreduce/lib/output/TextOutputFormat);
		TextOutputFormat.setOutputPath(job, new Path(conf.get("outpath")));
		return job.waitForCompletion(true) ? 0 : 1;
	}
}
