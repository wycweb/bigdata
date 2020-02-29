package com.wangyichao.bigdata.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object JoinApp01 {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName(this.getClass.getSimpleName)
      .setMaster("local[*]")
      .set("hive.metastore.uris", "thrift://bigdata001:9083")
      .set("spark.sql.warehouse.dir", "hdfs://bigdata001:8020/user/hive/warehouse")
      .set("spark.sql.parquet.binaryAsString", "true")
      .set("hive.exec.dynamic.partition", "true")
      .set("hive.exec.dynamic.partition.mode", "nonstrict")

    val sparkSession = SparkSession.builder()
      .config(conf)
      .enableHiveSupport()
      .getOrCreate()

    val testDF = sparkSession.table("test.user_test")

    testDF.show()

    sparkSession.close()
  }
}
