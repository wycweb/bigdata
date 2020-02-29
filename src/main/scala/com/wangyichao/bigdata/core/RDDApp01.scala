package com.wangyichao.bigdata.core

import org.apache.spark.{SparkConf, SparkContext}


/**
 * 如何构建一个spark应用程序
 */
object RDDApp01 {

  def main(args: Array[String]): Unit = {
    //The first thing a Spark program must do is to create a SparkContext object
    //SparkContext用来告诉spark用什么方式来运行

    //step1:拿到SparkConf
    val conf = new SparkConf()
//      .setMaster("local[2]")
//      .setAppName(this.getClass.getSimpleName)

    //step2: SparkContext
    val sc = new SparkContext(conf)


    //step3:业务逻辑处理

    //step4:关闭，一定要关，否则比如后期设置spark历史执行记录的话，代码运行完很可能不会保存Spark执行记录
    sc.stop()
  }

}
