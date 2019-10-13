package com.briup.grms.step2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class GoodsCooccurrenceMatrix2 extends Configured implements Tool {
	public static void main(String[] args)
			throws Exception {
		ToolRunner.run
				(new GoodsCooccurrenceMatrix2(),args);
	}
	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = getConf();
		Job job = Job.getInstance
				(conf,"zj_GoodsCooccurrenceMatrix");
		job.setJarByClass(this.getClass());

		job.setMapperClass(Mapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		job.setReducerClass(GCMReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setInputFormatClass
				(KeyValueTextInputFormat.class);
		KeyValueTextInputFormat.addInputPath
				(job,new Path(conf.get("inpath")));

		job.setOutputFormatClass
				(TextOutputFormat.class);
		TextOutputFormat.setOutputPath
				(job,new Path(conf.get("outpath")));

		job.waitForCompletion(true);
		return 0;
	}
	public static class GCMReducer
		extends Reducer<Text,Text,Text,Text>{
		@Override
		protected void reduce(Text key,
		    Iterable<Text> values, Context context) throws IOException, InterruptedException {
			Stream<Text> stream = StreamSupport.stream
				(values.spliterator(), false);
			//key gid  value  num
			Map<String, Long> map = stream.collect(
				Collectors.groupingBy
					(s -> s.toString(),
						 Collectors.counting()));
			StringBuffer sb = new StringBuffer();
			map.forEach(
				(k,v)->sb.append(k+":"+v).append(","));
			String str =
				sb.substring(0,sb.length()-1);
			context.write(key,new Text(str));
		}
	}

}
