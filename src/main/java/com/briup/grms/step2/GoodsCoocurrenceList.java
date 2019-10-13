package com.briup.grms.step2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
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

/**
 * 计算两两商品共现次数
 * 输入数据 ./grms/step1/
 * 输出数据 ./grms/step2/
 * 10001    20001,20002,20003.....
 *
 * 输出：
 * 20001    20002   3
 * 20002    20004   6
 *
 * 下一步我们需要把共现列表整理成矩阵形式
 * 考虑这一步，怎么做更方便下一步操作
 *
 */
public class GoodsCoocurrenceList extends Configured implements Tool {
    public static void main (String[] args) throws Exception{
        ToolRunner.run(new GoodsCoocurrenceList(),args);
    }

    @Override
    public int run(String[] strings) throws Exception {
        Configuration conf = getConf();
        Job job = Job.getInstance(conf,"zhf_GoodsCoocurrenceList");
        job.setJarByClass(this.getClass());
        job.setMapperClass(GCLMappper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setReducerClass(GCLReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job,new Path(conf.get("inpath")));

        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job,new Path(conf.get("outpath")));
        return job.waitForCompletion(true)?0:1;
    }
    public static class GCLMappper extends Mapper<LongWritable,Text,Text, IntWritable> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] values=value.toString().split("\t");
            String[] datas = values[1].split(",");
            for(int i=0;i<datas.length;i++){
                for(int j=0;j<datas.length;j++){
                    String pair = null;
                    pair=datas[i]+","+datas[j];
                    context.write(new Text(pair),new IntWritable(1));
                }
            }
        }
    }
    public static class GCLReducer extends Reducer<Text,IntWritable,Text,IntWritable>{
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for(IntWritable i:values){
                sum+=i.get();
            }
            context.write(key,new IntWritable(sum));
        }

    }
}
