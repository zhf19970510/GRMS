package com.briup.grms.main;

import com.briup.grms.step1.PurchasedGoodsList;
import com.briup.grms.step2.GetCurrent;
import com.briup.grms.step2.GoodsCoocurrenceList;
import com.briup.grms.step3.GetBuyerVector;
import com.briup.grms.step4.GetRecmandResult;
import com.briup.grms.step4.IdTag;
import com.briup.grms.step4.IdTagGroupComparator;
import com.briup.grms.step4.IdTagPartitioner;
import com.briup.grms.step5.MakeSumForMultiplication;
import com.briup.grms.step6.DuplicateDataForResult;
import com.briup.grms.step6.PreResult;
import com.briup.grms.step7.HdfsToDB;
import com.briup.grms.step7.UserProductRecommand;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

public class grms extends Configured implements Tool {
    public static void main(String[] args) throws Exception{
        ToolRunner.run(new grms(),args);
    }

    @Override
    public int run(String[] strings) throws Exception {
        System.out.println("hello world");
        Configuration conf = getConf();
        //清理上次运行的目录文件
        FileSystem fs = FileSystem.get(conf);
        fs.delete(new Path("grms/"),true);
        //删除数据库中的数据
        //配置数据库信息
        String driver="oracle.jdbc.driver.OracleDriver";
        String url="jdbc:oracle:thin:@172.16.14.146:1521:XE";
        String username="briup";
        String password="briup";
        DBConfiguration.configureDB(conf,driver,url,username,password);
        Class.forName(driver);
        System.out.println("not successful?");
        String sql1 = "select * from grms_results";
        String sql2 = "delete from grms_results";
        Connection conn = DriverManager.getConnection(url,username,password);

        Statement stmt = conn.createStatement();

        ResultSet rs = stmt.executeQuery(sql1);

        if(rs.next()){
            stmt.execute(sql2);
            conn.commit();
            if(stmt!=null){
                stmt.close();
            }
        }
        if(conn!=null){
            conn.close();
        }

        //获取用户购买向量  step1 - PurchasedGoodsList
        Job job1 = Job.getInstance(conf,"zhf_purchaseGoodsList");
        job1.setJarByClass(this.getClass());
        job1.setMapperClass(PurchasedGoodsList.PGMapper.class);
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(Text.class);

        job1.setReducerClass(PurchasedGoodsList.PGLReducer.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);

        job1.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job1,new Path("/data/rmc/process/matrix_data.txt"));

        job1.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job1,new Path("grms/step1"));

        //获得相似矩阵列表  step2 - GoodsCoocurrenceList
        Job job2 = Job.getInstance(conf,"zhf_GoodsCoocurrenceList");
        job2.setJarByClass(this.getClass());
        job2.setMapperClass(GoodsCoocurrenceList.GCLMappper.class);
        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(IntWritable.class);

        job2.setReducerClass(GoodsCoocurrenceList.GCLReducer.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(IntWritable.class);

        job2.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job2,new Path("grms/step1"));

        job2.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job2,new Path("grms/step2"));
        //获得相似矩阵step2 -GetCurrent
        Job job2_2 = Job.getInstance(conf,"zhf_GetCurrent");
        job2_2.setJarByClass(this.getClass());
        job2_2.setMapperClass(GetCurrent.GCMapper.class);
        job2_2.setMapOutputKeyClass(Text.class);
        job2_2.setMapOutputValueClass(Text.class);

        job2_2.setReducerClass(GetCurrent.GCReducer.class);
        job2_2.setOutputKeyClass(Text.class);
        job2_2.setOutputValueClass(Text.class);

        job2_2.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job2_2,new Path("grms/step2"));

        job2_2.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job2_2,new Path("grms/step2_2"));
        //获取转置的用户购买向量step3 - GetBuyerVector
        Job job3 = Job.getInstance(conf,"zhf_GetBuyerVector");
        job3.setJarByClass(this.getClass());
        job3.setMapperClass(GetBuyerVector.GBVMapper.class);
        job3.setMapOutputKeyClass(Text.class);
        job3.setMapOutputValueClass(Text.class);

        job3.setReducerClass(GetBuyerVector.GBVReducer.class);
        job3.setOutputKeyClass(Text.class);
        job3.setOutputValueClass(Text.class);

        job3.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job3,new Path("grms/step1"));

        job3.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job3,new Path("grms/step3"));
        //获取用户推荐结果关于推荐度的分向量 step4 - GetRecomandResult
        conf.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator", "\t");
        Job job4 = Job.getInstance(conf,"zenghongfa_GetRecmandResult");
        job4.setJarByClass(this.getClass());

        //定义路径
        Path firstIn4_1 = new Path("grms/step2_2");
        Path firstIn4_2 = new Path("grms/step3");

        Path outPath4 = new Path("grms/step4");

        MultipleInputs.addInputPath(job4,firstIn4_1, KeyValueTextInputFormat.class, GetRecmandResult.GRRFirstMapper.class);
        MultipleInputs.addInputPath(job4,firstIn4_2,KeyValueTextInputFormat.class, GetRecmandResult.GRRSecondMapper.class);

        FileOutputFormat.setOutputPath(job4,outPath4);

        //设置分区和分组方式
        job4.setPartitionerClass(IdTagPartitioner.class);
        job4.setGroupingComparatorClass(IdTagGroupComparator.class);

        //设置reduce类型
        job4.setReducerClass(GetRecmandResult.GRRReducer.class);
        job4.setMapOutputKeyClass(IdTag.class);
        job4.setMapOutputValueClass(Text.class);
        job4.setOutputKeyClass(Text.class);
        job4.setOutputValueClass(IntWritable.class);
        //对上一步所求结果的分向量进行求和  step5 - MakeSumForMultiplication
        Path inpath5 = new Path("grms/step4");
        Path outpath5 = new Path("grms/step5");

        Job job5 = Job.getInstance(conf,"zenghongfa_MakeSumForMultiplication");
        job5.setJarByClass(this.getClass());
        //装配mapper
        job5.setInputFormatClass(KeyValueTextInputFormat.class);
        job5.setOutputFormatClass(TextOutputFormat.class);


        KeyValueTextInputFormat.addInputPath(job5,inpath5);
        TextOutputFormat.setOutputPath(job5,outpath5);

        job5.setMapperClass(MakeSumForMultiplication.MakeSumForMultiplicationMapper.class);
        job5.setMapOutputKeyClass(Text.class);
        job5.setMapOutputValueClass(IntWritable.class);

//        job.setReducerClass(IntSumReducer.class);
        job5.setReducerClass(MakeSumForMultiplication.MakeSumForMultiplicationReducer.class);
        job5.setOutputKeyClass(Text.class);
        job5.setOutputValueClass(IntWritable.class);
        //为了去重，先将用户已经购买过的商品进行预处理 step6 - PreResult
        Job job6_1 = Job.getInstance(conf,"zenghongfa_PreResult");
        job6_1.setJarByClass(this.getClass());
        //设置输入输出路径
        Path inpath6_1 = new Path("/data/rmc/process/matrix_data.txt");
        Path outpath6_1 = new Path("grms/step6_1");

        //设置输入输出格式
        job6_1.setInputFormatClass(TextInputFormat.class);
        job6_1.setOutputFormatClass(TextOutputFormat.class);
        //加入路径
        TextInputFormat.addInputPath(job6_1,inpath6_1);
        TextOutputFormat.setOutputPath(job6_1,outpath6_1);
        //装配mapper
        job6_1.setMapperClass(PreResult.PRMapper.class);
        job6_1.setMapOutputKeyClass(Text.class);
        job6_1.setMapOutputValueClass(IntWritable.class);

        //装配reducer
        job6_1.setReducerClass(Reducer.class);
        job6_1.setOutputKeyClass(Text.class);
        job6_1.setOutputValueClass(IntWritable.class);
        //对用户已经购买过的商品推荐去除（即去重）step6 - DuplicateDataForResult
        Job job6_2 = Job.getInstance(conf,"zenghongfa_DuplicateDataForResult");
        job6_2.setJarByClass(this.getClass());
        //设置输入输出路径
        Path inpath6_21 = new Path("grms/step5");
        Path inpath6_22 = new Path("grms/step6_1");
        Path outpath6_2 = new Path("grms/step6_2");
        //设置输入输出格式
        job6_2.setInputFormatClass(KeyValueTextInputFormat.class);
        job6_2.setOutputFormatClass(TextOutputFormat.class);
        //加入路径
//      KeyValueTextInputFormat.addInputPath(job,inpath1);
//      KeyValueTextInputFormat.addInputPath(job,inpath2);
        FileInputFormat.setInputPaths(job6_2,inpath6_21,inpath6_22);
        TextOutputFormat.setOutputPath(job6_2,outpath6_2);
        //装配mapper
        job6_2.setMapperClass(DuplicateDataForResult.DuplicateDataForResultMapper.class);
        job6_2.setMapOutputKeyClass(Text.class);
        job6_2.setMapOutputValueClass(Text.class);

        //装配reducer
        job6_2.setReducerClass(DuplicateDataForResult.DuplicateDataForResultReducer.class);
        job6_2.setOutputKeyClass(Text.class);
        job6_2.setOutputValueClass(IntWritable.class);
        //将所得数据存入数据库中  step7 - HdfsToDB
        Job job7 = Job.getInstance(conf,"zhf_HdfsToDB");
        job7.setJarByClass(this.getClass());

        //设置输入输出路径
        Path inpath7 = new Path("grms/step6_2");

        //设置输入格式
        job7.setInputFormatClass(TextInputFormat.class);

        //加入路径
        TextInputFormat.addInputPath(job7,inpath7);

        //对jdbc进行配置
//        DBConfiguration.configureDB(job7.getConfiguration(),
//                "oracle.jdbc.driver.OracleDriver",
//                "jdbc:oracle:thin:@172.16.14.146:1521:XE",
//                "briup","briup");
        DBOutputFormat.setOutput(job7,
                "grms_results",
                "user_id","gid", "exp");
        job7.setOutputFormatClass(DBOutputFormat.class);

        //装配mapper
        job7.setMapperClass(HdfsToDB.HdfsToDBMapper.class);
        job7.setMapOutputKeyClass(UserProductRecommand.class);
        job7.setMapOutputValueClass(NullWritable.class);

        //因为不需要进行规约处理，将分区个数设置为0
        job7.setNumReduceTasks(0);

        ControlledJob job1_cj = new ControlledJob(job1.getConfiguration());
        ControlledJob job2_cj = new ControlledJob(job2.getConfiguration());
        ControlledJob job2_2_cj = new ControlledJob(job2_2.getConfiguration());
        ControlledJob job3_cj = new ControlledJob(job3.getConfiguration());
        ControlledJob job4_cj = new ControlledJob(job4.getConfiguration());
        ControlledJob job5_cj = new ControlledJob(job5.getConfiguration());
        ControlledJob job6_1_cj = new ControlledJob(job6_1.getConfiguration());
        ControlledJob job6_2_cj = new ControlledJob(job6_2.getConfiguration());
        ControlledJob job7_cj = new ControlledJob(job7.getConfiguration());

        job2_cj.addDependingJob(job1_cj);
        job2_2_cj.addDependingJob(job2_2_cj);
        job3_cj.addDependingJob(job1_cj);
        job4_cj.addDependingJob(job2_2_cj);
        job4_cj.addDependingJob(job3_cj);
        job5_cj.addDependingJob(job4_cj);
        job6_2_cj.addDependingJob(job5_cj);
        job6_2_cj.addDependingJob(job6_1_cj);
        job7_cj.addDependingJob(job2_2_cj);

        JobControl control = new JobControl("grmsCtrol");
        control.addJob(job1_cj);
        control.addJob(job2_cj);
        control.addJob(job2_2_cj);
        control.addJob(job3_cj);
        control.addJob(job4_cj);
        control.addJob(job5_cj);
        control.addJob(job6_1_cj);
        control.addJob(job6_2_cj);
        control.addJob(job7_cj);
        Thread thread = new Thread(control);
        thread.start();
        while (!control.allFinished()){

        }
        System.out.println("入库完毕");
        System.exit(0);
        return 0;

    }
}
