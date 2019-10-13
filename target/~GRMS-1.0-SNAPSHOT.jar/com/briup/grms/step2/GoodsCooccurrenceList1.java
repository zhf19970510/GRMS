// Decompiled by Jad v1.5.8e2. Copyright 2001 Pavel Kouznetsov.
// Jad home page: http://kpdus.tripod.com/jad.html
// Decompiler options: packimports(3) fieldsfirst ansi space 
// Source File Name:   GoodsCooccurrenceList1.java

package com.briup.grms.step2;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class GoodsCooccurrenceList1 extends Configured
	implements Tool
{
	public static class GCLMapper extends Mapper
	{

		protected void map(LongWritable key, Text value, org.apache.hadoop.mapreduce.Mapper.Context context)
			throws IOException, InterruptedException
		{
			String infos[] = value.toString().split("[\t]");
			String goods[] = infos[1].split("[,]");
			String as[] = goods;
			int i = as.length;
			for (int j = 0; j < i; j++)
			{
				String g1 = as[j];
				String as1[] = goods;
				int k = as1.length;
				for (int l = 0; l < k; l++)
				{
					String g2 = as1[l];
					context.write(new Text((new StringBuilder()).append(g1).append("\t").append(g2).toString()), NullWritable.get());
				}

			}

		}

		protected volatile void map(Object obj, Object obj1, org.apache.hadoop.mapreduce.Mapper.Context context)
			throws IOException, InterruptedException
		{
			map((LongWritable)obj, (Text)obj1, context);
		}

		public GCLMapper()
		{
		}
	}


	public GoodsCooccurrenceList1()
	{
	}

	public static void main(String args[])
		throws Exception
	{
		ToolRunner.run(new GoodsCooccurrenceList1(), args);
	}

	public int run(String args[])
		throws Exception
	{
		Configuration conf = getConf();
		Job job = Job.getInstance(conf, "zj_GoodsCooccurrenceList");
		job.setJarByClass(getClass());
		job.setMapperClass(com/briup/grms/step2/GoodsCooccurrenceList1$GCLMapper);
		job.setMapOutputKeyClass(org/apache/hadoop/io/Text);
		job.setMapOutputValueClass(org/apache/hadoop/io/NullWritable);
		job.setOutputKeyClass(org/apache/hadoop/io/Text);
		job.setOutputValueClass(org/apache/hadoop/io/NullWritable);
		job.setInputFormatClass(org/apache/hadoop/mapreduce/lib/input/TextInputFormat);
		TextInputFormat.addInputPath(job, new Path(conf.get("inpath")));
		job.setOutputFormatClass(org/apache/hadoop/mapreduce/lib/output/TextOutputFormat);
		TextOutputFormat.setOutputPath(job, new Path(conf.get("outpath")));
		job.waitForCompletion(true);
		return 0;
	}
}
