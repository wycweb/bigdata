package com.wangyichao.bigdata.core

import com.wangyichao.bigdata.utils.SparkContextUtil

object SortApp02 {
  def main(args: Array[String]): Unit = {

    val sc = SparkContextUtil.getSparkContext(this.getClass.getName)
    val products = sc.parallelize(List("黄金 100 5", "白金 200 4", "钻石 300 3"))

    products.map(x => {
      val splits = x.split(" ")
      val name = splits(0)
      val price = splits(1).toInt
      val amount = splits(2).toInt

      Products(name, price, amount)

    }).sortBy(x => x).foreach(println)


    sc.stop()
  }
}