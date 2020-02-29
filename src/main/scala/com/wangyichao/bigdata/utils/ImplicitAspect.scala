package com.wangyichao.bigdata.utils

import org.apache.spark.rdd.RDD

object ImplicitAspect {

  implicit def rddToRichRdd[T](rdd: RDD[T]): RichRDD[T] = new RichRDD[T](rdd)
}


class RichRDD[T](val rdd: RDD[T]) {

  def printInfo(flag: Int = 0): Unit = {
    if (flag == 0) {
      rdd.collect().foreach(println)
      println("~~~~~~~~~~")
    }
  }
}
