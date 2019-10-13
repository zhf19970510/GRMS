package com.briup.grms.step7;

import org.apache.commons.lang.ObjectUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

public class HdfsToDB extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        ToolRunner.run(new HdfsToDB(),args);
    }

    @Override
    public int run(String[] strings) throws Exception {
        Configuration conf = getConf();
        Job job = Job.getInstance(conf,"zhf_HdfsToDB");
        job.setJarByClass(this.getClass());

        //设置输入输出路径
        Path inpath = new Path(conf.get("inpath"));

        //设置输入格式
        job.setInputFormatClass(TextInputFormat.class);

        //加入路径
        TextInputFormat.addInputPath(job,inpath);

        //对jdbc进行配置
        DBConfiguration.configureDB(job.getConfiguration(),
                "oracle.jdbc.driver.OracleDriver",
                "jdbc:oracle:thin:@172.16.14.146:1521:XE",
                "briup","briup");
        DBOutputFormat.setOutput(job,
                "grms_results",
                "user_id","gid", "exp");
        job.setOutputFormatClass(DBOutputFormat.class);

        //装配mapper
        job.setMapperClass(HdfsToDBMapper.class);
        job.setMapOutputKeyClass(UserProductRecommand.class);
        job.setMapOutputValueClass(NullWritable.class);

        //因为不需要进行规约处理，将分区个数设置为0
        job.setNumReduceTasks(0);


        return job.waitForCompletion(true)?0:1;
    }

    public static class HdfsToDBMapper extends Mapper<LongWritable, Text,UserProductRecommand, NullWritable>{
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String strs[] = value.toString().split("\t");
            if(strs.length==2){
                String[] uidGid = strs[0].split(",");
                UserProductRecommand upr = new UserProductRecommand
                        (uidGid[0],uidGid[1],Integer.parseInt(strs[1]));
                context.write(upr,NullWritable.get());

            }
        }
    }
}
