package com.daoke360.task.review

import kafka.serializer.StringDecoder
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by 华硕电脑 on 2019/5/7.
  */
object KafkaReceiverDemo {




  def main(args: Array[String]): Unit = {
    //receiverDemo()
    //directDemo()
    directDemo2()
  }
  /**
    * Receiver模式
    */
  def receiverDemo(): Unit ={
    val sparkConf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[*]")
    val sc = new SparkContext(sparkConf)
    sc.setLogLevel("WARN")
    val ssc = new StreamingContext(sc,Seconds(2))
    //val inputDstream = KafkaUtils.createStream(ssc,"hadoopk1:2181,hadoopk2:2181","g1804",Map("wordcount"-> 3))
    val kafkaParams = Map[String, String](
      "zookeeper.connect" -> "hadoopk1:2181,hadoopk2:2181",
      "group.id" -> "g1804",
      /**
        * 当在zk上找不到消费偏移量时，这个配置项就会起作用，代表的是起始消费位置
        * 1,smallest :最早消息的偏移量
        * 2,largest：最新消息的偏移量(默认值)
        */
      "auto.offset.reset" -> "largest",
      //每隔多长时间更新一次消费偏移量(默认是60s)
      "auto.commit.interval.ms" -> "1000"
    )
    val inputDstream = KafkaUtils.createStream[String,String,StringDecoder,StringDecoder](ssc,kafkaParams,Map("wordcount" -> 3), StorageLevel.MEMORY_ONLY)
    val wordCountDstream = inputDstream.map(_._2).flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_)
    wordCountDstream.print()
    ssc.start()
    ssc.awaitTermination()
  }
  /**
    * Direct模式（直联）
    */
  def directDemo():Unit = {
    val sparkConf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[*]")
    val sc = new SparkContext(sparkConf)
    sc.setLogLevel("WARN")
    val ssc = new StreamingContext(sc,Seconds(3))
    val kafkaParams = Map[String, String](
      //kafka broker 地址
      "metadata.broker.list" -> "hadoopk1:9092,hadoopk2:9092",
      "group.id" -> "G1804",
      "auto.offset.reset" -> "largest"
    )
    kafkaParams
    KafkaUtils.createDirectStream[String,String,StringDecoder,StringDecoder](ssc,kafkaParams,Set("wordcount"))
      .map(_._2).flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_).print()
    ssc.start()
    ssc.awaitTermination()
  }



  def directDemo2():Unit = {
    val checkpointPath = "/checkpoint/G1804"
    val sparkConf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[*]")
    val sc = new SparkContext(sparkConf)
    sc.setLogLevel("WARN")
    /**
      * checkpointPath保存了一下基本信息：
      * 1，streaming 应用的配置信息 sparkConf
      * 2,保存了dstream的操作转换逻辑关系
      * 3,保存了未处理完的batch元数据信息
      * 4,保存了消费偏移量
      */
    val ssc = StreamingContext.getOrCreate(checkpointPath,()=>createStreamingContext(sc,checkpointPath))
    ssc.start()
    ssc.awaitTermination()
  }
  def createStreamingContext(sc: SparkContext, checkpointPath: String):StreamingContext = {
    val ssc = new StreamingContext(sc,Seconds(3))
    ssc.checkpoint(checkpointPath)

    val kafkaParams = Map[String, String](
      //kafka broker地址
      "metadata.broker.list" -> "hadoopk1:9092,hadoopk1:9092",
      "group.id" -> "G1804",
      "auto.offset.reset" -> "largest"
    )
    KafkaUtils.createDirectStream[String,String,StringDecoder,StringDecoder](ssc,kafkaParams,Set("wordcount")).map(_._2).flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_).print()
    ssc
    }
}
