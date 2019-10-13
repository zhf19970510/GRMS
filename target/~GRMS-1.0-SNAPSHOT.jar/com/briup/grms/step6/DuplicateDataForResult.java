// Decompiled by Jad v1.5.8e2. Copyright 2001 Pavel Kouznetsov.
// Jad home page: http://kpdus.tripod.com/jad.html
// Decompiler options: packimports(3) fieldsfirst ansi space 
// Source File Name:   DuplicateDataForResult.java

package com.briup.grms.step6;

import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class DuplicateDataForResult extends Configured
	implements Tool
{
	public static class DuplicateDataForResultReducer extends Reducer
	{

		protected void reduce(Text key, Iterable values, org.apache.hadoop.mapreduce.Reducer.Context context)
			throws IOException, InterruptedException
		{
			int sum = 0;
			StringBuffer sb = new StringBuffer();
			Text v;
			for (Iterator iterator = values.iterator(); iterator.hasNext(); sb.append(v.toString()))
			{
				v = (Text)iterator.next();
				sum++;
			}

			if (sum == 1)
			{
				int result = Integer.parseInt(sb.toString());
				context.write(key, new IntWritable(result));
			}
		}

		protected volatile void reduce(Object obj, Iterable iterable, org.apache.hadoop.mapreduce.Reducer.Context context)
			throws IOException, InterruptedException
		{
			reduce((Text)obj, iterable, context);
		}

		public DuplicateDataForResultReducer()
		{
		}
	}

	public static class DuplicateDataForResultMapper extends Mapper
	{

		protected void map(Text key, Text value, org.apache.hadoop.mapreduce.Mapper.Context context)
			throws IOException, InterruptedException
		{
			String strs[] = key.toString().split(",");
			context.write(new Text((new StringBuilder()).append(strs[1]).append(",").append(strs[0]).toString()), value);
		}

		protected volatile void map(Object obj, Object obj1, org.apache.hadoop.mapreduce.Mapper.Context context)
			throws IOException, InterruptedException
		{
			map((Text)obj, (Text)obj1, context);
		}

		public DuplicateDataForResultMapper()
		{
		}
	}


	public DuplicateDataForResult()
	{
	}

	public static void main(String args[])
		throws Exception
	{
		ToolRunner.run(new DuplicateDataForResult(), args);
	}

	public int run(String strings[])
		throws Exception
	{
		Configuration conf = getConf();
		conf.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator", "\t");
		Job job = Job.getInstance(conf, "zenghongfa_DuplicateDataForResult");
		job.setJarByClass(getClass());
		Path inpath1 = new Path(conf.get("inpath1"));
		Path inpath2 = new Path(conf.get("inpath2"));
		Path outpath = new Path(conf.get("outpath"));
		job.setInputFormatClass(org/apache/hadoop/mapreduce/lib/input/KeyValueTextInputFormat);
		job.setOutputFormatClass(org/apache/hadoop/mapreduce/lib/output/TextOutputFormat);
		FileInputFormat.setInputPaths(job, new Path[] {
			inpath1, inpath2
		});
		TextOutputFormat.setOutputPath(job, outpath);
		job.setMapperClass(com/briup/grms/step6/DuplicateDataForResult$DuplicateDataForResultMapper);
		job.setMapOutputKeyClass(org/apache/hadoop/io/Text);
		job.setMapOutputValueClass(org/apache/hadoop/io/Text);
		job.setReducerClass(com/briup/grms/step6/DuplicateDataForResult$DuplicateDataForResultReducer);
		job.setOutputKeyClass(org/apache/hadoop/io/Text);
		job.setOutputValueClass(org/apache/hadoop/io/IntWritable);
		return job.waitForCompletion(true) ? 0 : 1;
	}
}
