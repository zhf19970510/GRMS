// Decompiled by Jad v1.5.8e2. Copyright 2001 Pavel Kouznetsov.
// Jad home page: http://kpdus.tripod.com/jad.html
// Decompiler options: packimports(3) fieldsfirst ansi space 
// Source File Name:   GetRecmandResult.java

package com.briup.grms.step4;

import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

// Referenced classes of package com.briup.grms.step4:
//			IdTagPartitioner, IdTagGroupComparator, IdTag

public class GetRecmandResult extends Configured
	implements Tool
{
	public static class GRRReducer extends Reducer
	{

		protected void reduce(IdTag key, Iterable values, org.apache.hadoop.mapreduce.Reducer.Context context)
			throws IOException, InterruptedException
		{
			StringBuffer sb = new StringBuffer();
			Text t;
			for (Iterator iterator = values.iterator(); iterator.hasNext(); sb.append(t.toString()).append("-"))
				t = (Text)iterator.next();

			String v = sb.substring(0, sb.length() - 1);
			String args[] = v.split("-");
			if (args.length == 2)
			{
				String goods[] = args[0].split(" ");
				String customers[] = args[1].split(" ");
				for (int i = 0; i < goods.length; i++)
				{
					for (int j = 0; j < customers.length; j++)
					{
						String goodsPres[] = goods[i].split(":");
						String customer[] = customers[j].split(":");
						String outkey = (new StringBuilder()).append(goodsPres[0]).append(",").append(customer[0]).toString();
						int outvalue = Integer.parseInt(goodsPres[1]) * Integer.parseInt(customer[1]);
						context.write(new Text(outkey), new IntWritable(outvalue));
					}

				}

			}
		}

		protected volatile void reduce(Object obj, Iterable iterable, org.apache.hadoop.mapreduce.Reducer.Context context)
			throws IOException, InterruptedException
		{
			reduce((IdTag)obj, iterable, context);
		}

		public GRRReducer()
		{
		}
	}

	public static class GRRSecondMapper extends Mapper
	{

		protected void map(Text key, Text value, org.apache.hadoop.mapreduce.Mapper.Context context)
			throws IOException, InterruptedException
		{
			context.write(new IdTag(key.toString(), 2), value);
		}

		protected volatile void map(Object obj, Object obj1, org.apache.hadoop.mapreduce.Mapper.Context context)
			throws IOException, InterruptedException
		{
			map((Text)obj, (Text)obj1, context);
		}

		public GRRSecondMapper()
		{
		}
	}

	public static class GRRFirstMapper extends Mapper
	{

		protected void map(Text key, Text value, org.apache.hadoop.mapreduce.Mapper.Context context)
			throws IOException, InterruptedException
		{
			context.write(new IdTag(key.toString(), 1), value);
		}

		protected volatile void map(Object obj, Object obj1, org.apache.hadoop.mapreduce.Mapper.Context context)
			throws IOException, InterruptedException
		{
			map((Text)obj, (Text)obj1, context);
		}

		public GRRFirstMapper()
		{
		}
	}


	public GetRecmandResult()
	{
	}

	public static void main(String args[])
		throws Exception
	{
		ToolRunner.run(new GetRecmandResult(), args);
	}

	public int run(String strings[])
		throws Exception
	{
		Configuration conf = getConf();
		conf.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator", "\t");
		Job job = Job.getInstance(conf, "zenghongfa_GetRecmandResult");
		job.setJarByClass(com/briup/grms/step4/GetRecmandResult);
		Path firstIn1 = new Path(conf.get("firstIn1"));
		Path firstIn2 = new Path(conf.get("firstIn2"));
		Path outPath = new Path(conf.get("outpath"));
		MultipleInputs.addInputPath(job, firstIn1, org/apache/hadoop/mapreduce/lib/input/KeyValueTextInputFormat, com/briup/grms/step4/GetRecmandResult$GRRFirstMapper);
		MultipleInputs.addInputPath(job, firstIn2, org/apache/hadoop/mapreduce/lib/input/KeyValueTextInputFormat, com/briup/grms/step4/GetRecmandResult$GRRSecondMapper);
		FileOutputFormat.setOutputPath(job, outPath);
		job.setPartitionerClass(com/briup/grms/step4/IdTagPartitioner);
		job.setGroupingComparatorClass(com/briup/grms/step4/IdTagGroupComparator);
		job.setReducerClass(com/briup/grms/step4/GetRecmandResult$GRRReducer);
		job.setMapOutputKeyClass(com/briup/grms/step4/IdTag);
		job.setMapOutputValueClass(org/apache/hadoop/io/Text);
		job.setOutputKeyClass(org/apache/hadoop/io/Text);
		job.setOutputValueClass(org/apache/hadoop/io/IntWritable);
		return job.waitForCompletion(true) ? 0 : 1;
	}
}
