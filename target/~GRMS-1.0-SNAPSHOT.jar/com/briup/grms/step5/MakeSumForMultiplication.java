// Decompiled by Jad v1.5.8e2. Copyright 2001 Pavel Kouznetsov.
// Jad home page: http://kpdus.tripod.com/jad.html
// Decompiler options: packimports(3) fieldsfirst ansi space 
// Source File Name:   MakeSumForMultiplication.java

package com.briup.grms.step5;

import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class MakeSumForMultiplication extends Configured
	implements Tool
{
	public static class MakeSumForMultiplicationReducer extends Reducer
	{

		protected void reduce(Text key, Iterable values, org.apache.hadoop.mapreduce.Reducer.Context context)
			throws IOException, InterruptedException
		{
			int sum = 0;
			for (Iterator iterator = values.iterator(); iterator.hasNext();)
			{
				IntWritable v = (IntWritable)iterator.next();
				sum += v.get();
			}

			context.write(key, new IntWritable(sum));
		}

		protected volatile void reduce(Object obj, Iterable iterable, org.apache.hadoop.mapreduce.Reducer.Context context)
			throws IOException, InterruptedException
		{
			reduce((Text)obj, iterable, context);
		}

		public MakeSumForMultiplicationReducer()
		{
		}
	}

	public static class MakeSumForMultiplicationMapper extends Mapper
	{

		protected void map(Text key, Text value, org.apache.hadoop.mapreduce.Mapper.Context context)
			throws IOException, InterruptedException
		{
			context.write(key, new IntWritable(Integer.parseInt(value.toString())));
		}

		protected volatile void map(Object obj, Object obj1, org.apache.hadoop.mapreduce.Mapper.Context context)
			throws IOException, InterruptedException
		{
			map((Text)obj, (Text)obj1, context);
		}

		public MakeSumForMultiplicationMapper()
		{
		}
	}


	public MakeSumForMultiplication()
	{
	}

	public static void main(String args[])
		throws Exception
	{
		ToolRunner.run(new MakeSumForMultiplication(), args);
	}

	public int run(String strings[])
		throws Exception
	{
		Configuration conf = getConf();
		conf.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator", "\t");
		Path inpath = new Path(conf.get("inpath"));
		Path outpath = new Path(conf.get("outpath"));
		Job job = Job.getInstance(conf, "zenghongfa_MakeSumForMultiplication");
		job.setJarByClass(getClass());
		job.setInputFormatClass(org/apache/hadoop/mapreduce/lib/input/KeyValueTextInputFormat);
		job.setOutputFormatClass(org/apache/hadoop/mapreduce/lib/output/TextOutputFormat);
		KeyValueTextInputFormat.addInputPath(job, inpath);
		TextOutputFormat.setOutputPath(job, outpath);
		job.setMapperClass(com/briup/grms/step5/MakeSumForMultiplication$MakeSumForMultiplicationMapper);
		job.setMapOutputKeyClass(org/apache/hadoop/io/Text);
		job.setMapOutputValueClass(org/apache/hadoop/io/IntWritable);
		job.setReducerClass(com/briup/grms/step5/MakeSumForMultiplication$MakeSumForMultiplicationReducer);
		job.setOutputKeyClass(org/apache/hadoop/io/Text);
		job.setOutputValueClass(org/apache/hadoop/io/IntWritable);
		return job.waitForCompletion(true) ? 0 : 1;
	}
}
