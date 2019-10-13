package com.briup.grms.step4;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import sun.reflect.annotation.ExceptionProxy;
import sun.security.krb5.Config;

import java.awt.font.LayoutPath;
import java.io.IOException;

/**
 * 计算对于某个商品对于某个商品的推荐值
 * 借助  MultipleInputs 类把两个Mapper输出的
 * 汇聚结果输出到同一个reduce
 * 2.需要使用到二次排除
 *  构建复合键  Comparable
 *  分区比较器
 *  分组比较器
 *
 */
public class GetRecmandResult extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
    	ToolRunner.run(new GetRecmandResult(),args);
    }

    @Override
    public int run(String[] strings) throws Exception {
        Configuration conf = getConf();
        conf.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator", "\t");
        Job job = Job.getInstance(conf,"zenghongfa_GetRecmandResult");
        job.setJarByClass(GetRecmandResult.class);

        //定义路径
        Path firstIn1 = new Path(conf.get("firstIn1"));
        Path firstIn2 = new Path(conf.get("firstIn2"));

        Path outPath = new Path(conf.get("outpath"));

        MultipleInputs.addInputPath(job,firstIn1,KeyValueTextInputFormat.class,GRRFirstMapper.class);
        MultipleInputs.addInputPath(job,firstIn2,KeyValueTextInputFormat.class,GRRSecondMapper.class);

        FileOutputFormat.setOutputPath(job,outPath);

        //设置分区和分组方式
        job.setPartitionerClass(IdTagPartitioner.class);
        job.setGroupingComparatorClass(IdTagGroupComparator.class);

        //设置reduce类型
        job.setReducerClass(GRRReducer.class);
        job.setMapOutputKeyClass(IdTag.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        return job.waitForCompletion(true)?0:1;
    }

    public static class GRRFirstMapper extends Mapper<Text,Text,IdTag,Text>{
        @Override
        protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            context.write(new IdTag(key.toString(),1),value);

        }
    }

    public static class GRRSecondMapper extends  Mapper<Text,Text,IdTag,Text>{
        @Override
        protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            context.write(new IdTag(key.toString(),2),value);

        }
    }

    /**
     * map阶段的输出数据有
     *
     * id,tag  20001:2 20002:3 2005:1 ... 10001:1 10003:1...
     * 需要进行累加计算对吧
     *
     */
    public static class GRRReducer extends Reducer<IdTag,Text,Text, IntWritable>{
        @Override
        protected void reduce(IdTag key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
//            for(Text t:values) {
//                context.write(new Text(key.getId().toString()),t);
//            }
            StringBuffer sb = new StringBuffer();
            for(Text t:values){
                sb.append(t.toString()).append("-");
            }
            String v = sb.substring(0,sb.length()-1);
            String[] args = v.split("-");
            if(args.length==2) {
                String[] goods = args[0].split(" ");
                String[] customers = args[1].split(" ");
                for(int i = 0;i<goods.length;i++){
                    for(int j=0;j<customers.length;j++){
                        String[] goodsPres =goods[i].split(":");
                        String[] customer = customers[j].split(":");
                        String outkey = goodsPres[0]+","+customer[0];
                        int outvalue = Integer.parseInt(goodsPres[1])*
                                Integer.parseInt(customer[1]);

                        context.write(new Text(outkey),new IntWritable(outvalue));


                    }
                }
            }

        }
    }
}
