package com.wangyichao.bigdata.core

import com.wangyichao.bigdata.utils.SparkContextUtil
import com.wangyichao.bigdata.utils.ImplicitAspect._

object CoalesceAndRepartitionApp01 {

  def main(args: Array[String]): Unit = {

    val sc = SparkContextUtil.getSparkContext(this.getClass.getSimpleName)

    val rdd1 = sc.parallelize(List(1 to 12: _*), 3)

    println(rdd1.partitions.size)

    rdd1.mapPartitionsWithIndex((index, partition) => {
      partition.map(x => s"分区是：$index，元素是：$x")
    }).printInfo()

    /**
     * 创建一个新的数据，用新的数据指定分区。它能够增加或减少RDD的并行度。
     * 本质上是通过shuffle生成新的数据。
     */
    val repartitionRDD = rdd1.repartition(4)

    repartitionRDD.mapPartitionsWithIndex((index, partition) => {
      partition.map(x => s"分区是：$index，元素是：$x")
    }).printInfo()


  }
}
