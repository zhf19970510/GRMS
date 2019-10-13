package com.briup.grms.step2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

public class GetCurrent extends Configured implements Tool {
    public static void main(String[] args) throws Exception{
        ToolRunner.run(new GetCurrent(),args);
    }
    @Override
    public int run(String[] strings) throws Exception {
        Configuration conf = getConf();
        Job job = Job.getInstance(conf,"zhf_GetCurrent");
        job.setJarByClass(this.getClass());
        job.setMapperClass(GCMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setReducerClass(GCReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job,new Path(conf.get("inpath")));

        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job,new Path(conf.get("outpath")));
        return job.waitForCompletion(true)?0:1;
    }
    public static class GCMapper extends Mapper<LongWritable,Text,Text,Text>{
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String str=value.toString();
            String[] strs = str.split(",");
            String[]values = strs[1].split("\t");
            String s = values[0]+":"+values[1];
            context.write(new Text(strs[0]),new Text(s));
        }
    }
    public static class GCReducer extends Reducer<Text,Text,Text,Text>{
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String str = values.toString();
            StringBuffer sb = new StringBuffer();
            for(Text value:values){
                sb.append(value).append(" ");
            }
            context.write(key,new Text(sb.toString()));
        }
    }
}
