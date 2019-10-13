// Decompiled by Jad v1.5.8e2. Copyright 2001 Pavel Kouznetsov.
// Jad home page: http://kpdus.tripod.com/jad.html
// Decompiler options: packimports(3) fieldsfirst ansi space 
// Source File Name:   GoodsCooccurrenceMatrix2.java

package com.briup.grms.step2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class GoodsCooccurrenceMatrix2 extends Configured
	implements Tool
{
	/* member class not found */
	class GCMReducer {}


	public GoodsCooccurrenceMatrix2()
	{
	}

	public static void main(String args[])
		throws Exception
	{
		ToolRunner.run(new GoodsCooccurrenceMatrix2(), args);
	}

	public int run(String args[])
		throws Exception
	{
		Configuration conf = getConf();
		Job job = Job.getInstance(conf, "zj_GoodsCooccurrenceMatrix");
		job.setJarByClass(getClass());
		job.setMapperClass(org/apache/hadoop/mapreduce/Mapper);
		job.setMapOutputKeyClass(org/apache/hadoop/io/Text);
		job.setMapOutputValueClass(org/apache/hadoop/io/Text);
		job.setReducerClass(com/briup/grms/step2/GoodsCooccurrenceMatrix2$GCMReducer);
		job.setOutputKeyClass(org/apache/hadoop/io/Text);
		job.setOutputValueClass(org/apache/hadoop/io/Text);
		job.setInputFormatClass(org/apache/hadoop/mapreduce/lib/input/KeyValueTextInputFormat);
		KeyValueTextInputFormat.addInputPath(job, new Path(conf.get("inpath")));
		job.setOutputFormatClass(org/apache/hadoop/mapreduce/lib/output/TextOutputFormat);
		TextOutputFormat.setOutputPath(job, new Path(conf.get("outpath")));
		job.waitForCompletion(true);
		return 0;
	}
}
