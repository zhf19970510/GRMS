package com.briup.grms.step6;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

public class PreResult extends Configured implements Tool {

    public static void main(String[] args) throws Exception{
        ToolRunner.run(new PreResult(),args);
    }

    @Override
    public int run(String[] strings) throws Exception {
        Configuration conf = getConf();
        Job job = Job.getInstance(conf,"zenghongfa_PreResult");
        job.setJarByClass(this.getClass());
        //设置输入输出路径
        Path inpath = new Path(conf.get("inpath"));
        Path outpath = new Path(conf.get("outpath"));

        //设置输入输出格式
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        //加入路径
        TextInputFormat.addInputPath(job,inpath);
        TextOutputFormat.setOutputPath(job,outpath);
        //装配mapper
        job.setMapperClass(PRMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        //装配reducer
        job.setReducerClass(Reducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        return job.waitForCompletion(true)?0:1;
    }
    //从原始文件中获取数据
    public static class PRMapper extends Mapper<LongWritable, Text,Text, IntWritable> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String s1 = value.toString();
            String[] strs = s1.split(" ");
            context.write(new Text(strs[1]+","+strs[0]),new IntWritable(1));
        }

    }
}
