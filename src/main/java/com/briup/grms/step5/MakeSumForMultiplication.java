package com.briup.grms.step5;

import com.sun.xml.internal.xsom.ForeignAttributes;
import com.sun.xml.internal.xsom.impl.scd.Iterators;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.reduce.IntSumReducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.kerby.config.Conf;

import java.io.IOException;
import java.time.temporal.ValueRange;

public class MakeSumForMultiplication extends Configured implements Tool {

    public static void main(String[] args) throws  Exception{
        ToolRunner.run(new MakeSumForMultiplication(), args);
    }
    @Override
    public int run(String[] strings) throws Exception {
        Configuration conf = getConf();
        conf.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator", "\t");

        Path inpath = new Path(conf.get("inpath"));
        Path outpath = new Path(conf.get("outpath"));

        Job job = Job.getInstance(conf,"zenghongfa_MakeSumForMultiplication");
        job.setJarByClass(this.getClass());
        //装配mapper
        job.setInputFormatClass(KeyValueTextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);


        KeyValueTextInputFormat.addInputPath(job,inpath);
        TextOutputFormat.setOutputPath(job,outpath);

        job.setMapperClass(MakeSumForMultiplicationMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

//        job.setReducerClass(IntSumReducer.class);
        job.setReducerClass(MakeSumForMultiplicationReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);



        return job.waitForCompletion(true)?0:1;
    }
    public static class MakeSumForMultiplicationMapper extends Mapper<Text,Text,Text,IntWritable>{
        @Override
        protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            context.write(key,new IntWritable(
                    Integer.parseInt(value.toString())));
        }

    }

    public static class MakeSumForMultiplicationReducer extends Reducer<Text,IntWritable,Text,IntWritable>{
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for(IntWritable v:values){
                sum +=v.get();
            }

            context.write(key,new IntWritable(sum));
        }

    }

}
