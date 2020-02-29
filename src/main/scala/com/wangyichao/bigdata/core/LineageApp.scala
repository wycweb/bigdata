package com.wangyichao.bigdata.core

import com.wangyichao.bigdata.utils.SparkContextUtil
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

/**
 * RDD之间的依赖关系
 * 在Spark中叫做 Lineage，用来描述RDD是如何从父RDD过来的
 * 性能、操作
 *
 * 在spark中 可以称为 compute chain
 *
 */
object LineageApp {

  def main(args: Array[String]): Unit = {
    val sc = SparkContextUtil.getSparkContext(this.getClass.getSimpleName)

    val a = sc.parallelize(List(1, 2, 3, 4, 5))
    val b = a.map(_ * 2)
    val c = b.filter(_ > 6)

    c.collect().foreach(println)


    /**
     * 1.首先textFile读文件 hadoopFile(
     * path,                        //路径
     * classOf[TextInputFormat],    //输入类型为文本类型
     * classOf[LongWritable],       //
     * classOf[Text],               //文本
     * minPartitions)
     *
     *
     * sc.textFile其实就是MapReduce的一个map内容，map下的偏移量对于离线处理是没有任何作用的
     * 所有sc.textFile后面，在读取文本后，进行了异步 .map(pair => pair._2.toString).setName(path)，即将偏移量抛弃掉的操作
     *
     * MapReduce 有 InputFormat
     * mapper:LongWritable(每行数据的偏移量) Text(数据的偏移量)
     *
     *
     * 总结：textFile最后生成的事HadoopRDD,跟踪HadoopRDD的 getPartitions 方法可以看到，HadoopRDD的分区数量，就是Hadoop文件的分片数量
     * 为什么要讲解这个textFile读取：这个代码的读取流程，代码难度适当，不负责，但也有完整的实现流程，对理解Spark中RDD的分区规则会有很好的帮助
     *
     * 因为textFile中后面会有map操作，所以还会产生一个MapPartitionRDD
     *
     *
     * 下面整个代码的RDD关系
     * textFile：
     * HadoopRDD
     * MapPartitionRDD
     * flatMap:
     * MapPartitionRDD
     * map:
     * MapPartitionRDD
     * reduceByKey:
     * ShuffledRdd
     *
     */
    val input = sc.textFile("file:///Users/wangyichao/code/bigdata/data/wc.text")
    val words = input.flatMap(_.split(","))
    val pair = words.map((_, 1))


    /**
     * reduceByKey 是有map端预聚合的功能的
     * groupByKey 没有map端预聚合的功能，需要所有数据全部经过shuffle处理
     */
    val result = pair.reduceByKey(_ + _)
    val result1 = pair.groupByKey().mapValues(_.sum)


    result.cache()
    result.persist(StorageLevel.MEMORY_ONLY_SER)

    result.unpersist()

    val resultDebugString = input.toDebugString

    result.collect().foreach(println)

    println(resultDebugString)

  }
}
