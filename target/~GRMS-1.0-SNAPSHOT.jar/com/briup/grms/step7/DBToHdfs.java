// Decompiled by Jad v1.5.8e2. Copyright 2001 Pavel Kouznetsov.
// Jad home page: http://kpdus.tripod.com/jad.html
// Decompiler options: packimports(3) fieldsfirst ansi space 
// Source File Name:   DBToHdfs.java

package com.briup.grms.step7;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

// Referenced classes of package com.briup.grms.step7:
//			UserProductRecommand

public class DBToHdfs extends Configured
	implements Tool
{
	public static class ShowTableMapper extends Mapper
	{

		protected void map(LongWritable key, UserProductRecommand value, org.apache.hadoop.mapreduce.Mapper.Context context)
			throws IOException, InterruptedException
		{
			context.write(key, value);
		}

		protected volatile void map(Object obj, Object obj1, org.apache.hadoop.mapreduce.Mapper.Context context)
			throws IOException, InterruptedException
		{
			map((LongWritable)obj, (UserProductRecommand)obj1, context);
		}

		public ShowTableMapper()
		{
		}
	}


	public DBToHdfs()
	{
	}

	public static void main(String args[])
		throws Exception
	{
		ToolRunner.run(new DBToHdfs(), args);
	}

	public int run(String strings[])
		throws Exception
	{
		Configuration conf = getConf();
		Job job = Job.getInstance(conf, "zhf_DBToHdfs");
		job.setJarByClass(getClass());
		DBConfiguration.configureDB(job.getConfiguration(), "oracle.jdbc.driver.OracleDriver", "jdbc:oracle:thin:@192.168.43.156:1521:XE", "briup", "briup");
		DBInputFormat.setInput(job, com/briup/grms/step7/UserProductRecommand, "grms_results", "rownum<=10", "uid", new String[] {
			"uid", "gid", "exp"
		});
		job.setInputFormatClass(org/apache/hadoop/mapreduce/lib/db/DBInputFormat);
		job.setOutputFormatClass(org/apache/hadoop/mapreduce/lib/output/TextOutputFormat);
		TextOutputFormat.setOutputPath(job, new Path(conf.get("outpath")));
		job.setMapperClass(com/briup/grms/step7/DBToHdfs$ShowTableMapper);
		job.setMapOutputKeyClass(org/apache/hadoop/io/LongWritable);
		job.setMapOutputValueClass(com/briup/grms/step7/UserProductRecommand);
		job.setNumReduceTasks(0);
		return job.waitForCompletion(true) ? 0 : 1;
	}
}
