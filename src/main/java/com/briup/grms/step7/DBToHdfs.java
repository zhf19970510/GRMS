package com.briup.grms.step7;

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

import java.io.IOException;

public class DBToHdfs extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        ToolRunner.run(new DBToHdfs(),args);
    }

    @Override
    public int run(String[] strings) throws Exception {
        Configuration conf = getConf();
        Job job = Job.getInstance(conf,"zhf_DBToHdfs");
        job.setJarByClass(this.getClass());

        //配置DB
        DBConfiguration.configureDB(job.getConfiguration(), "oracle.jdbc.driver.OracleDriver",
                "jdbc:oracle:thin:@192.168.43.156:1521:XE","briup","briup");
        DBInputFormat.setInput(job, UserProductRecommand.class,
                "grms_results",
                "rownum<=10",
                "uid",
                "uid","gid","exp");
        //设置输入输出格式
        job.setInputFormatClass(DBInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        //设置输出路径
        TextOutputFormat.setOutputPath(job,new Path(conf.get("outpath")));

        //装配mappper
        job.setMapperClass(ShowTableMapper.class);
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(UserProductRecommand.class);
        job.setNumReduceTasks(0);

        return job.waitForCompletion(true)?0:1;
    }
    public static class ShowTableMapper extends Mapper<LongWritable,UserProductRecommand,LongWritable,UserProductRecommand>{
        @Override
        protected void map(LongWritable key, UserProductRecommand value, Context context) throws IOException, InterruptedException {
            context.write(key,value);
        }
    }
}
