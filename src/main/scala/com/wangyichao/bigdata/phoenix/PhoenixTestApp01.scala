package com.wangyichao.bigdata.phoenix

import org.apache.spark.SparkContext
import org.apache.spark.sql.{SaveMode, SparkSession}

object PhoenixTestApp01 {

  private val table = "USER"
  private val zkUrl = "bigdata001,bigdata002,bigdata003:2181"

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("phoenix-test")
      .master("local")
      .getOrCreate()

    val sqlContext = spark.sqlContext

    val df = sqlContext.read.format("org.apache.phoenix.spark")
      .option("table", table)
      .option("zkUrl", zkUrl)
      .load()

    val finaleDF = df.filter("ID != 1")

    finaleDF.write.format("org.apache.phoenix.spark")
      .mode(SaveMode.Overwrite)
      .option("zkUrl", zkUrl)
      .option("table", table)
      .save();

  }
}
