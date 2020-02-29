package com.wangyichao.bigdata.core

import com.wangyichao.bigdata.utils.SparkContextUtil

object RDDActionApp01 {

  def main(args: Array[String]): Unit = {

    val sc = SparkContextUtil.getSparkContext(this.getClass.getName)

    val rdd = sc.parallelize(List(1, 2, 3, 4, 5))
    val rdd2 = sc.parallelize(List(("a", 1), ("b", 2), ("c", 3), ("d", 4)), 2)


    //全局排序
    rdd2.sortBy(_._2, false).foreach(println)

    rdd.count()
    rdd.reduce(_ + _)
    rdd.first()
    rdd.take(2)
    rdd.top(2)

    rdd.zipWithIndex()

    //统计key的个数
    rdd2.countByKey()



  }

}
