// Decompiled by Jad v1.5.8e2. Copyright 2001 Pavel Kouznetsov.
// Jad home page: http://kpdus.tripod.com/jad.html
// Decompiler options: packimports(3) fieldsfirst ansi space 
// Source File Name:   grms.java

package com.briup.grms.main;

import com.briup.grms.step1.PurchasedGoodsList;
import com.briup.grms.step2.GetCurrent;
import com.briup.grms.step2.GoodsCoocurrenceList;
import com.briup.grms.step3.GetBuyerVector;
import com.briup.grms.step4.*;
import com.briup.grms.step5.MakeSumForMultiplication;
import com.briup.grms.step6.DuplicateDataForResult;
import com.briup.grms.step6.PreResult;
import com.briup.grms.step7.HdfsToDB;
import com.briup.grms.step7.UserProductRecommand;
import java.io.PrintStream;
import java.sql.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class grms extends Configured
	implements Tool
{

	public grms()
	{
	}

	public static void main(String args[])
		throws Exception
	{
		ToolRunner.run(new grms(), args);
	}

	public int run(String strings[])
		throws Exception
	{
		System.out.println("hello world");
		Configuration conf = getConf();
		FileSystem fs = FileSystem.get(conf);
		fs.delete(new Path("grms/"), true);
		String driver = "oracle.jdbc.driver.OracleDriver";
		String url = "jdbc:oracle:thin:@172.16.14.146:1521:XE";
		String username = "briup";
		String password = "briup";
		DBConfiguration.configureDB(conf, driver, url, username, password);
		Class.forName(driver);
		System.out.println("not successful?");
		String sql1 = "select * from grms_results";
		String sql2 = "delete from grms_results";
		Connection conn = DriverManager.getConnection(url, username, password);
		Statement stmt = conn.createStatement();
		ResultSet rs = stmt.executeQuery(sql1);
		if (rs.next())
		{
			stmt.execute(sql2);
			conn.commit();
			if (stmt != null)
				stmt.close();
		}
		if (conn != null)
			conn.close();
		Job job1 = Job.getInstance(conf, "zhf_purchaseGoodsList");
		job1.setJarByClass(getClass());
		job1.setMapperClass(com/briup/grms/step1/PurchasedGoodsList$PGMapper);
		job1.setMapOutputKeyClass(org/apache/hadoop/io/Text);
		job1.setMapOutputValueClass(org/apache/hadoop/io/Text);
		job1.setReducerClass(com/briup/grms/step1/PurchasedGoodsList$PGLReducer);
		job1.setOutputKeyClass(org/apache/hadoop/io/Text);
		job1.setOutputValueClass(org/apache/hadoop/io/Text);
		job1.setInputFormatClass(org/apache/hadoop/mapreduce/lib/input/TextInputFormat);
		TextInputFormat.addInputPath(job1, new Path("/data/rmc/process/matrix_data.txt"));
		job1.setOutputFormatClass(org/apache/hadoop/mapreduce/lib/output/TextOutputFormat);
		TextOutputFormat.setOutputPath(job1, new Path("grms/step1"));
		Job job2 = Job.getInstance(conf, "zhf_GoodsCoocurrenceList");
		job2.setJarByClass(getClass());
		job2.setMapperClass(com/briup/grms/step2/GoodsCoocurrenceList$GCLMappper);
		job2.setMapOutputKeyClass(org/apache/hadoop/io/Text);
		job2.setMapOutputValueClass(org/apache/hadoop/io/IntWritable);
		job2.setReducerClass(com/briup/grms/step2/GoodsCoocurrenceList$GCLReducer);
		job2.setOutputKeyClass(org/apache/hadoop/io/Text);
		job2.setOutputValueClass(org/apache/hadoop/io/IntWritable);
		job2.setInputFormatClass(org/apache/hadoop/mapreduce/lib/input/TextInputFormat);
		TextInputFormat.addInputPath(job2, new Path("grms/step1"));
		job2.setOutputFormatClass(org/apache/hadoop/mapreduce/lib/output/TextOutputFormat);
		TextOutputFormat.setOutputPath(job2, new Path("grms/step2"));
		Job job2_2 = Job.getInstance(conf, "zhf_GetCurrent");
		job2_2.setJarByClass(getClass());
		job2_2.setMapperClass(com/briup/grms/step2/GetCurrent$GCMapper);
		job2_2.setMapOutputKeyClass(org/apache/hadoop/io/Text);
		job2_2.setMapOutputValueClass(org/apache/hadoop/io/Text);
		job2_2.setReducerClass(com/briup/grms/step2/GetCurrent$GCReducer);
		job2_2.setOutputKeyClass(org/apache/hadoop/io/Text);
		job2_2.setOutputValueClass(org/apache/hadoop/io/Text);
		job2_2.setInputFormatClass(org/apache/hadoop/mapreduce/lib/input/TextInputFormat);
		TextInputFormat.addInputPath(job2_2, new Path("grms/step2"));
		job2_2.setOutputFormatClass(org/apache/hadoop/mapreduce/lib/output/TextOutputFormat);
		TextOutputFormat.setOutputPath(job2_2, new Path("grms/step2_2"));
		Job job3 = Job.getInstance(conf, "zhf_GetBuyerVector");
		job3.setJarByClass(getClass());
		job3.setMapperClass(com/briup/grms/step3/GetBuyerVector$GBVMapper);
		job3.setMapOutputKeyClass(org/apache/hadoop/io/Text);
		job3.setMapOutputValueClass(org/apache/hadoop/io/Text);
		job3.setReducerClass(com/briup/grms/step3/GetBuyerVector$GBVReducer);
		job3.setOutputKeyClass(org/apache/hadoop/io/Text);
		job3.setOutputValueClass(org/apache/hadoop/io/Text);
		job3.setInputFormatClass(org/apache/hadoop/mapreduce/lib/input/TextInputFormat);
		TextInputFormat.addInputPath(job3, new Path("grms/step1"));
		job3.setOutputFormatClass(org/apache/hadoop/mapreduce/lib/output/TextOutputFormat);
		TextOutputFormat.setOutputPath(job3, new Path("grms/step3"));
		conf.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator", "\t");
		Job job4 = Job.getInstance(conf, "zenghongfa_GetRecmandResult");
		job4.setJarByClass(getClass());
		Path firstIn4_1 = new Path("grms/step2_2");
		Path firstIn4_2 = new Path("grms/step3");
		Path outPath4 = new Path("grms/step4");
		MultipleInputs.addInputPath(job4, firstIn4_1, org/apache/hadoop/mapreduce/lib/input/KeyValueTextInputFormat, com/briup/grms/step4/GetRecmandResult$GRRFirstMapper);
		MultipleInputs.addInputPath(job4, firstIn4_2, org/apache/hadoop/mapreduce/lib/input/KeyValueTextInputFormat, com/briup/grms/step4/GetRecmandResult$GRRSecondMapper);
		FileOutputFormat.setOutputPath(job4, outPath4);
		job4.setPartitionerClass(com/briup/grms/step4/IdTagPartitioner);
		job4.setGroupingComparatorClass(com/briup/grms/step4/IdTagGroupComparator);
		job4.setReducerClass(com/briup/grms/step4/GetRecmandResult$GRRReducer);
		job4.setMapOutputKeyClass(com/briup/grms/step4/IdTag);
		job4.setMapOutputValueClass(org/apache/hadoop/io/Text);
		job4.setOutputKeyClass(org/apache/hadoop/io/Text);
		job4.setOutputValueClass(org/apache/hadoop/io/IntWritable);
		Path inpath5 = new Path("grms/step4");
		Path outpath5 = new Path("grms/step5");
		Job job5 = Job.getInstance(conf, "zenghongfa_MakeSumForMultiplication");
		job5.setJarByClass(getClass());
		job5.setInputFormatClass(org/apache/hadoop/mapreduce/lib/input/KeyValueTextInputFormat);
		job5.setOutputFormatClass(org/apache/hadoop/mapreduce/lib/output/TextOutputFormat);
		KeyValueTextInputFormat.addInputPath(job5, inpath5);
		TextOutputFormat.setOutputPath(job5, outpath5);
		job5.setMapperClass(com/briup/grms/step5/MakeSumForMultiplication$MakeSumForMultiplicationMapper);
		job5.setMapOutputKeyClass(org/apache/hadoop/io/Text);
		job5.setMapOutputValueClass(org/apache/hadoop/io/IntWritable);
		job5.setReducerClass(com/briup/grms/step5/MakeSumForMultiplication$MakeSumForMultiplicationReducer);
		job5.setOutputKeyClass(org/apache/hadoop/io/Text);
		job5.setOutputValueClass(org/apache/hadoop/io/IntWritable);
		Job job6_1 = Job.getInstance(conf, "zenghongfa_PreResult");
		job6_1.setJarByClass(getClass());
		Path inpath6_1 = new Path("/data/rmc/process/matrix_data.txt");
		Path outpath6_1 = new Path("grms/step6_1");
		job6_1.setInputFormatClass(org/apache/hadoop/mapreduce/lib/input/TextInputFormat);
		job6_1.setOutputFormatClass(org/apache/hadoop/mapreduce/lib/output/TextOutputFormat);
		TextInputFormat.addInputPath(job6_1, inpath6_1);
		TextOutputFormat.setOutputPath(job6_1, outpath6_1);
		job6_1.setMapperClass(com/briup/grms/step6/PreResult$PRMapper);
		job6_1.setMapOutputKeyClass(org/apache/hadoop/io/Text);
		job6_1.setMapOutputValueClass(org/apache/hadoop/io/IntWritable);
		job6_1.setReducerClass(org/apache/hadoop/mapreduce/Reducer);
		job6_1.setOutputKeyClass(org/apache/hadoop/io/Text);
		job6_1.setOutputValueClass(org/apache/hadoop/io/IntWritable);
		Job job6_2 = Job.getInstance(conf, "zenghongfa_DuplicateDataForResult");
		job6_2.setJarByClass(getClass());
		Path inpath6_21 = new Path("grms/step5");
		Path inpath6_22 = new Path("grms/step6_1");
		Path outpath6_2 = new Path("grms/step6_2");
		job6_2.setInputFormatClass(org/apache/hadoop/mapreduce/lib/input/KeyValueTextInputFormat);
		job6_2.setOutputFormatClass(org/apache/hadoop/mapreduce/lib/output/TextOutputFormat);
		FileInputFormat.setInputPaths(job6_2, new Path[] {
			inpath6_21, inpath6_22
		});
		TextOutputFormat.setOutputPath(job6_2, outpath6_2);
		job6_2.setMapperClass(com/briup/grms/step6/DuplicateDataForResult$DuplicateDataForResultMapper);
		job6_2.setMapOutputKeyClass(org/apache/hadoop/io/Text);
		job6_2.setMapOutputValueClass(org/apache/hadoop/io/Text);
		job6_2.setReducerClass(com/briup/grms/step6/DuplicateDataForResult$DuplicateDataForResultReducer);
		job6_2.setOutputKeyClass(org/apache/hadoop/io/Text);
		job6_2.setOutputValueClass(org/apache/hadoop/io/IntWritable);
		Job job7 = Job.getInstance(conf, "zhf_HdfsToDB");
		job7.setJarByClass(getClass());
		Path inpath7 = new Path("grms/step6_2");
		job7.setInputFormatClass(org/apache/hadoop/mapreduce/lib/input/TextInputFormat);
		TextInputFormat.addInputPath(job7, inpath7);
		DBOutputFormat.setOutput(job7, "grms_results", new String[] {
			"user_id", "gid", "exp"
		});
		job7.setOutputFormatClass(org/apache/hadoop/mapreduce/lib/db/DBOutputFormat);
		job7.setMapperClass(com/briup/grms/step7/HdfsToDB$HdfsToDBMapper);
		job7.setMapOutputKeyClass(com/briup/grms/step7/UserProductRecommand);
		job7.setMapOutputValueClass(org/apache/hadoop/io/NullWritable);
		job7.setNumReduceTasks(0);
		ControlledJob job1_cj = new ControlledJob(job1.getConfiguration());
		ControlledJob job2_cj = new ControlledJob(job2.getConfiguration());
		ControlledJob job2_2_cj = new ControlledJob(job2_2.getConfiguration());
		ControlledJob job3_cj = new ControlledJob(job3.getConfiguration());
		ControlledJob job4_cj = new ControlledJob(job4.getConfiguration());
		ControlledJob job5_cj = new ControlledJob(job5.getConfiguration());
		ControlledJob job6_1_cj = new ControlledJob(job6_1.getConfiguration());
		ControlledJob job6_2_cj = new ControlledJob(job6_2.getConfiguration());
		ControlledJob job7_cj = new ControlledJob(job7.getConfiguration());
		job2_cj.addDependingJob(job1_cj);
		job2_2_cj.addDependingJob(job2_2_cj);
		job3_cj.addDependingJob(job1_cj);
		job4_cj.addDependingJob(job2_2_cj);
		job4_cj.addDependingJob(job3_cj);
		job5_cj.addDependingJob(job4_cj);
		job6_2_cj.addDependingJob(job5_cj);
		job6_2_cj.addDependingJob(job6_1_cj);
		job7_cj.addDependingJob(job2_2_cj);
		JobControl control = new JobControl("grmsCtrol");
		control.addJob(job1_cj);
		control.addJob(job2_cj);
		control.addJob(job2_2_cj);
		control.addJob(job3_cj);
		control.addJob(job4_cj);
		control.addJob(job5_cj);
		control.addJob(job6_1_cj);
		control.addJob(job6_2_cj);
		control.addJob(job7_cj);
		Thread thread = new Thread(control);
		thread.start();
		while (!control.allFinished()) ;
		System.out.println("»Îø‚ÕÍ±œ");
		System.exit(0);
		return 0;
	}
}
