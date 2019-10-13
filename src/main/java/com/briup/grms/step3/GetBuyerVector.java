package com.briup.grms.step3;

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

public class GetBuyerVector extends Configured implements Tool {

    public static void main(String[] args) throws Exception{
        ToolRunner.run(new GetBuyerVector(),args);
    }

    @Override
    public int run(String[] strings) throws Exception {
        Configuration conf = getConf();
        Job job = Job.getInstance(conf,"zhf_GetBuyerVector");
        job.setJarByClass(this.getClass());
        job.setMapperClass(GBVMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setReducerClass(GBVReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job,new Path(conf.get("inpath")));

        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job,new Path(conf.get("outpath")));
        return job.waitForCompletion(true)?0:1;
    }

    public static class GBVMapper extends Mapper<LongWritable, Text,Text,Text>{
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] strs = value.toString().split("\t");
            //获取各个商品
            String[] keys = strs[1].split(",");
            //将矩阵转置
            for(String goods:keys) {
                //strs[0]表示uid
                context.write(new Text(goods), new Text(strs[0]));
            }
        }
    }
    //将购买同样商品的用户打印输出，并计数1
    public static class GBVReducer extends Reducer<Text,Text,Text,Text>{
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            StringBuffer sb = new StringBuffer();
            for(Text t:values){
                sb.append(t).append(":1").append(" ");
            }
            context.write(key,new Text(sb.toString()));
        }

    }

}
