package com.wangyichao.bigdata.utils

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Duration, Seconds, StreamingContext}

object SparkContextUtil {


  def getSparkContext(appName: String): SparkContext = {
    val conf = new SparkConf()
      .setMaster("local[2]")
      .setAppName(appName)

    val sc = new SparkContext(conf)

    sc
  }

  /**
   * 获取SparkContext基本配置
   *
   * @param setMaster
   * @return
   */
  def getSparkSession(setMaster: String = "local[2]"): SparkContext = {
    val sc = SparkSession.builder()
      .appName(this.getClass.getName)
      .master(setMaster)
      .getOrCreate()
      .sparkContext

    sc
  }

  /**
   * 获取StreaminContext基本配置
   *
   * @param setMaster
   * @param batchDur
   * @return
   */
  def getSparkStreamingContext(setMaster: String = "local[2]", batchDur: Duration = Seconds(3)): StreamingContext = {
    val sc = this.getSparkSession(setMaster)

    val ssc = new StreamingContext(sc, batchDur)

    ssc
  }


}
