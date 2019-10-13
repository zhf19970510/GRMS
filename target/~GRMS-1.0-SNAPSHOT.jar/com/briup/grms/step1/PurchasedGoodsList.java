// Decompiled by Jad v1.5.8e2. Copyright 2001 Pavel Kouznetsov.
// Jad home page: http://kpdus.tripod.com/jad.html
// Decompiler options: packimports(3) fieldsfirst ansi space 
// Source File Name:   PurchasedGoodsList.java

package com.briup.grms.step1;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class PurchasedGoodsList extends Configured
	implements Tool
{
	/* member class not found */
	class PGLReducer {}

	public static class PGMapper extends Mapper
	{

		protected void map(LongWritable key, Text value, org.apache.hadoop.mapreduce.Mapper.Context context)
			throws IOException, InterruptedException
		{
			String infos[] = value.toString().split(" ");
			context.write(new Text(infos[0]), new Text(infos[1]));
		}

		protected volatile void map(Object obj, Object obj1, org.apache.hadoop.mapreduce.Mapper.Context context)
			throws IOException, InterruptedException
		{
			map((LongWritable)obj, (Text)obj1, context);
		}

		public PGMapper()
		{
		}
	}


	public PurchasedGoodsList()
	{
	}

	public static void main(String args[])
		throws Exception
	{
		ToolRunner.run(new PurchasedGoodsList(), args);
	}

	public int run(String strings[])
		throws Exception
	{
		Configuration conf = getConf();
		Job job = Job.getInstance(conf, "zhf_purchaseGoodsList");
		job.setJarByClass(getClass());
		job.setMapperClass(com/briup/grms/step1/PurchasedGoodsList$PGMapper);
		job.setMapOutputKeyClass(org/apache/hadoop/io/Text);
		job.setMapOutputValueClass(org/apache/hadoop/io/Text);
		job.setReducerClass(com/briup/grms/step1/PurchasedGoodsList$PGLReducer);
		job.setOutputKeyClass(org/apache/hadoop/io/Text);
		job.setOutputValueClass(org/apache/hadoop/io/Text);
		job.setInputFormatClass(org/apache/hadoop/mapreduce/lib/input/TextInputFormat);
		TextInputFormat.addInputPath(job, new Path(conf.get("inpath")));
		job.setOutputFormatClass(org/apache/hadoop/mapreduce/lib/output/TextOutputFormat);
		TextOutputFormat.setOutputPath(job, new Path(conf.get("outpath")));
		return job.waitForCompletion(true) ? 0 : 1;
	}
}
