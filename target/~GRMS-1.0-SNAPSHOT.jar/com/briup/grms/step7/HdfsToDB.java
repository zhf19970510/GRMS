// Decompiled by Jad v1.5.8e2. Copyright 2001 Pavel Kouznetsov.
// Jad home page: http://kpdus.tripod.com/jad.html
// Decompiler options: packimports(3) fieldsfirst ansi space 
// Source File Name:   HdfsToDB.java

package com.briup.grms.step7;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

// Referenced classes of package com.briup.grms.step7:
//			UserProductRecommand

public class HdfsToDB extends Configured
	implements Tool
{
	public static class HdfsToDBMapper extends Mapper
	{

		protected void map(LongWritable key, Text value, org.apache.hadoop.mapreduce.Mapper.Context context)
			throws IOException, InterruptedException
		{
			String strs[] = value.toString().split("\t");
			if (strs.length == 2)
			{
				String uidGid[] = strs[0].split(",");
				UserProductRecommand upr = new UserProductRecommand(uidGid[0], uidGid[1], Integer.parseInt(strs[1]));
				context.write(upr, NullWritable.get());
			}
		}

		protected volatile void map(Object obj, Object obj1, org.apache.hadoop.mapreduce.Mapper.Context context)
			throws IOException, InterruptedException
		{
			map((LongWritable)obj, (Text)obj1, context);
		}

		public HdfsToDBMapper()
		{
		}
	}


	public HdfsToDB()
	{
	}

	public static void main(String args[])
		throws Exception
	{
		ToolRunner.run(new HdfsToDB(), args);
	}

	public int run(String strings[])
		throws Exception
	{
		Configuration conf = getConf();
		Job job = Job.getInstance(conf, "zhf_HdfsToDB");
		job.setJarByClass(getClass());
		Path inpath = new Path(conf.get("inpath"));
		job.setInputFormatClass(org/apache/hadoop/mapreduce/lib/input/TextInputFormat);
		TextInputFormat.addInputPath(job, inpath);
		DBConfiguration.configureDB(job.getConfiguration(), "oracle.jdbc.driver.OracleDriver", "jdbc:oracle:thin:@172.16.14.146:1521:XE", "briup", "briup");
		DBOutputFormat.setOutput(job, "grms_results", new String[] {
			"user_id", "gid", "exp"
		});
		job.setOutputFormatClass(org/apache/hadoop/mapreduce/lib/db/DBOutputFormat);
		job.setMapperClass(com/briup/grms/step7/HdfsToDB$HdfsToDBMapper);
		job.setMapOutputKeyClass(com/briup/grms/step7/UserProductRecommand);
		job.setMapOutputValueClass(org/apache/hadoop/io/NullWritable);
		job.setNumReduceTasks(0);
		return job.waitForCompletion(true) ? 0 : 1;
	}
}
