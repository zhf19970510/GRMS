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
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

public class DuplicateDataForResult extends Configured implements Tool {

    public static void main(String[] args) throws Exception{
        ToolRunner.run(new DuplicateDataForResult(),args);
    }

    @Override
    public int run(String[] strings) throws Exception {
        Configuration conf = getConf();
        conf.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator", "\t");
        Job job = Job.getInstance(conf,"zenghongfa_DuplicateDataForResult");
        job.setJarByClass(this.getClass());
        //设置输入输出路径
        Path inpath1 = new Path(conf.get("inpath1"));
        Path inpath2 = new Path(conf.get("inpath2"));
        Path outpath = new Path(conf.get("outpath"));
        //设置输入输出格式
        job.setInputFormatClass(KeyValueTextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        //加入路径
//      KeyValueTextInputFormat.addInputPath(job,inpath1);
//      KeyValueTextInputFormat.addInputPath(job,inpath2);
        FileInputFormat.setInputPaths(job,inpath1,inpath2);
        TextOutputFormat.setOutputPath(job,outpath);
        //装配mapper
        job.setMapperClass(DuplicateDataForResultMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        //装配reducer
        job.setReducerClass(DuplicateDataForResultReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        return job.waitForCompletion(true)?0:1;
    }

    public static class DuplicateDataForResultMapper extends Mapper<Text,Text,Text,Text>{
        @Override
        protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            String[] strs = key.toString().split(",");
            context.write(new Text(strs[1]+","+strs[0]),value);
        }
    }

    public static class DuplicateDataForResultReducer extends Reducer<Text,Text,Text,IntWritable>{
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            //如果有两个或两个以上的values则代表有重复，应该剔除，只保留有一个value的数据
            int sum = 0;
            StringBuffer sb = new StringBuffer();
            for(Text v:values){
                sum++;
                sb.append(v.toString());
            }
            if(sum==1){
                int result = Integer.parseInt(sb.toString());
                context.write(key,new IntWritable(result));
            }
        }
    }
}
