package com.wangyichao.bigdata;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;


import java.util.Arrays;
import java.util.List;

public class TestSpark {

    public static void main(String[] args) {

        //step1:拿到SparkConf
        SparkConf conf = new SparkConf()
                .setAppName("app_name")
                .setMaster("local[2]");

        //step2: SparkContext
        JavaSparkContext sc = new JavaSparkContext(conf);


        //step3:业务逻辑处理

        JavaRDD<String> distFile = sc.textFile("data.txt");

        List<Integer> data = Arrays.asList(1, 2, 3, 4, 5);

        JavaRDD<Integer> rdd = sc.parallelize(data);

        rdd.map(s -> s * 2);

        System.out.println(rdd.toDebugString());

        //step4:关闭，一定要关，否则比如后期设置spark历史执行记录的话，代码运行完很可能不会保存Spark执行记录
        sc.stop();

    }
}
