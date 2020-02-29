package com.wangyichao.bigdata.hadoop.mapreduce;

import com.wangyichao.bigdata.utils.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * MapReduce单词统计
 */
public class WordCountApp {

    /**
     * job启动类，设置参数并集群中提交job
     *
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {

        //面向套路变成，所有的MapReduce作业都是这么提交

        //step1:获取job对象
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);

        String input = "data/wc.data";
        String output = "output/";

        FileUtils.deleteTarget(output, configuration);

        //step2:获取job相关信息
        job.setJarByClass(WordCountApp.class);

        //step3:设置自定义的Mapper和Reducer
        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);
        job.setCombinerClass(MyReducer.class);

        //step4:设置Mapper阶段输出的key和value类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        //step5:设置reducer阶段输出的key和value类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        /**
         * FileInputFormat
         * FileInputFormat是基于文本的基础类
         * FileInputFormat 继承了 InputFormat
         * InputFormat是读数据最顶层的基础。
         * InputFormat 描述的事输入的数据给Map-Reduce的job
         *
         */
        //step6:设置输入输出路径
        FileInputFormat.setInputPaths(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));

        //step7:提交job
        /**
         * waitForCompletion做了什么事情
         */
        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);
    }

    /**
     * 自定义Mapper继承：org.apache.hadoop.mapreduce.Mapper，实现map方法
     *
     * 总体实现思路
     * 1.读取文件
     * 2.每行数据是有分隔符的，这里的分隔符是逗号
     * 3.每个单词赋值为1 map阶段
     * 4.shuffler 相同的key分发到一个reduce下
     * 5.数据分发，数据的归并
     */


    /**
     * KEYIN 输入数据key的数据类型 每行数据的偏移量
     * KEYVALUE 输入数据value的数据类型
     * KEYOUT 输出数据key的数据类型
     * VALUEOUT 输出数据value的数据类型
     */
    public static class MyMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

        IntWritable ONE = new IntWritable(1);

        /**
         * map 阶段完成的事情
         * 1） 文件的内容读取进来
         * 2） 每行数据是有分隔符的，这里的分隔符是逗号
         * 3）每个单词赋值为1
         */
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] splits = value.toString().split(",");

            for (String split : splits) {
                context.write(new Text(split), ONE);
            }
        }
    }

    /**
     * 自定义Reducer继承:org.apache.hadoop.mapreduce.Reducer，实现reduce方法
     */
    public static class MyReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

        /**
         * @param word
         * @param values 为什么是values 是Iterable 的，原因是shuffler过程，相同的key会分发到一个reduce上
         *               例如 a,<1,1,1>  b,<1,1> c,<1>
         */
        @Override
        protected void reduce(Text word, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int count = 0;

            for (IntWritable value : values) {
                count += value.get();
            }

            context.write(word, new IntWritable(count));
        }
    }
}