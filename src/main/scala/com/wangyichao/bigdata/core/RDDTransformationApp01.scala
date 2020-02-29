package com.wangyichao.bigdata.core

import com.wangyichao.bigdata.utils.SparkContextUtil
import org.apache.spark.rdd.RDD

/**
 * RDD转换算子
 */
object RDDTransformationApp01 {

  def main(args: Array[String]): Unit = {

    val sc = SparkContextUtil.getSparkContext(this.getClass.getSimpleName)

    //创建RDD
    val rdd = sc.parallelize(List(1, 2, 3, 4, 5))

    val rdd1 = rdd.map(_ * 2)

    rdd1.filter(_ != 1)


    rdd1.count()

    rdd1.collect().foreach(println)

    rdd1.collect().foreach(x => {
      println(x)
    })

    rdd1.collect().foreach(x => println(x))
    rdd1.collect().foreach(println(_))
    rdd1.collect().foreach(println)

    /**
     * 对于100个元素来说 10个分区元素
     * map要进行100次执行操作，MapPartitions只会执行10次
     * 这个在讲闭包的时候会去讲
     */
    rdd.map(_ * 2)
    rdd.mapPartitions(partition => partition.map(_ * 2)) //对每个分区进行处理
    rdd.mapPartitionsWithIndex((index, partition) => { //生产上绝对不会看是那个分区，所以这个函数生产上不会去用的
      partition.map(s"分区编号是${index},元素是" + _)
    })

    val rdd2 = sc.parallelize(List(("王艺超", 24), ("徐乐", 26)))
    val rdd3 = sc.parallelize(List(List(1, 2), List(3, 4)))

    rdd2.mapValues(_ + 1) //对value操作，key不动的

    rdd3.flatMap(x => {
      x.map(_ * 2)
    })


    //把每个分区的内容放在一个数组中 生产上不会用
    sc.parallelize(1 to 30).glom()
    //取样,第一个参数表述数据是否还放回去
    sc.parallelize(1 to 30).sample(true, 0.4, 2)

    sc.parallelize(1 to 30).filter(x => x % 2 == 0 && x > 10)

    rdd1.union(rdd) //合并
    rdd1.intersection(rdd) //交集
    rdd1.subtract(rdd) //差集
    rdd1.distinct() //去重 distinct重新分区的方式是元素对分区个数取模


    val rdd4 = sc.parallelize(List(("a", 1), ("b", 1), ("c", 3), ("a", 99)))

    //groupByKey很少用
    rdd4.groupByKey().mapValues(_.sum)

    rdd4.reduceByKey(_ + _)

    //拓展：distinct做的是去重操作，在不使用distinct怎么去重
    rdd.map((_, null)).reduceByKey((x, y) => x).map(_._1)


    //groupBy 自定义分组 分组条件就是传进去的
    sc.parallelize(List("a", "a", "a", "b", "b", "c", "d", "d")).groupBy(x => x).mapValues(x => x.size)

    //sortBy
    sc.parallelize(List(("王艺超", 24), ("徐乐", 26))).sortBy(_._2)


    val rdd5 = sc.parallelize(List(("aaa", "北京"), ("bbb", "上海"), ("ccc", "广州")))
    val rdd6 = sc.parallelize(List(("aaa", 30), ("bbb", 50), ("ddd", 100)))

    /**
     * 所有的join底层都是使用的cogroup
     *
     * 根据key进行关联，返回两边RDD的记录，没关联上是空
     * join返回值类型 RDD[K , (Option[V],Option[W])]
     * cogroup返回值类型 RDD[(K，（Iterable[V],Iterable[W]) )]
     */
    val value: RDD[(String, (String, Int))] = rdd5.join(rdd6)
    val value1: RDD[(String, (String, Option[Int]))] = rdd5.leftOuterJoin(rdd6)
    val value2: RDD[(String, (Option[String], Int))] = rdd5.rightOuterJoin(rdd6)
    val value3: RDD[(String, (Option[String], Option[Int]))] = rdd5.fullOuterJoin(rdd6)
    val value4: RDD[(String, (Iterable[String], Iterable[Int]))] = rdd5.cogroup(rdd6)



    //step3:业务逻辑处理

    //step4:关闭，一定要关，否则比如后期设置spark历史执行记录的话，代码运行完很可能不会保存Spark执行记录
    sc.stop()
  }

}
