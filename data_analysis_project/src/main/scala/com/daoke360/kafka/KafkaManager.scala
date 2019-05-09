package com.daoke360.kafka

import java.util.concurrent.atomic.AtomicReference

import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.Decoder
import org.apache.spark.SparkException
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaCluster, KafkaUtils, OffsetRange}

import scala.collection.mutable
import scala.reflect.ClassTag

/**
  * Created by 华硕电脑 on 2019/5/8.
  */
class KafkaManager(var kafkaParams: Map[String,String],topics:Set[String]) extends Serializable{
 //kafka的客户端api
   val kafkaCluster = new KafkaCluster(kafkaParams)
  //程序启动后立即调用,用来设置或更新消费偏移量
  setOrUpdateOffset()




  /**
    * 设置或更新消费偏移量
    */
  def setOrUpdateOffset()={
    //默认有消费偏移量
    var isConsume:Boolean=true
    //获取topic分区元数据信息
    val topicAndPartitions = getPartitions()
    //通过分区信息,查找对应的消费偏移量
    val errOrConsumerOffsets = kafkaCluster.getConsumerOffsets(kafkaParams("group.id"),topicAndPartitions)
    //判断是否有消费偏移量
    if(errOrConsumerOffsets.isLeft){
      isConsume=false
    }
    /**
      * 1,找不到消费偏移量，auto.offset.reset=smallest 那么这个map存放的是每个分区最早消息的偏移量，作为初始化的消费偏移量
      * 2,找不到消费偏移量，auto.offset.reset=largest 那么这个map存放的是每个分区最新消息的偏移量，作为初始化的消费偏移量
      * 3,找到消费偏移量，并且有的分区消费偏移量过期了，那么这个map里面存放的是更新后的消费偏移量
      */
      val setOrUpdateMap = mutable.Map[TopicAndPartition,Long]()
    if(isConsume){
      //有消费偏移量
      //取出消费偏移量
      val consumeOffsetsMap:Map[TopicAndPartition,Long] = errOrConsumerOffsets.right.get
      //取出每个分区最早消息偏移量
      val earliestLeaderOffsetsMap = getEarliestLeaderOffsets(topicAndPartitions)
      consumeOffsetsMap.foreach(t2 =>{
        //当前分区
        val topicAndPartition = t2._1
        //当前分区消费偏移量
        val consumeOffset = t2._2
        //当前分区最早消息偏移量
        val earliestLeaderOffset = earliestLeaderOffsetsMap(topicAndPartition).offset
        if(consumeOffset<earliestLeaderOffset){
          //消费偏移量过时了
          setOrUpdateMap.put(topicAndPartition,earliestLeaderOffset)
        }
      })
    }else{
      //没有消费偏移量
      val offsetReset = kafkaParams.getOrElse("auto.offset.reset","largest")
      if(offsetReset.equals("smallest")){
        val earliestLeaderOffsetsMap = getEarliestLeaderOffsets(topicAndPartitions)
        earliestLeaderOffsetsMap.foreach(t2 =>{
          //当前分区
          val topicAndPartition = t2._1
          //当前分区最早消息偏移量
          val earliestLeaderOffset = t2._2.offset
          setOrUpdateMap.put(topicAndPartition,earliestLeaderOffset)
        })
      }else{
        //获取每个分区最新消息偏移量
        val latestLeaderOffsetsMap = getLatestLeaderOffsets(topicAndPartitions)
        latestLeaderOffsetsMap.foreach(t2 =>{
          //当前分区
          val topicAndPartition = t2._1
          //当前分区最新消息偏移量
          val latestLeaderOffset = t2._2.offset
          setOrUpdateMap.put(topicAndPartition,latestLeaderOffset)
        })
      }
    }
    //设置或更新消费偏移
    kafkaCluster.setConsumerOffsets(kafkaParams("group.id"),setOrUpdateMap.toMap)
  }

  /**
    * 获取消费偏移量
    */
  def getConsumeOffsets(): Map[TopicAndPartition,Long]={
    //尝试获取消费偏移量
    val errOrConsumerOffsets = kafkaCluster.getConsumerOffsets(kafkaParams("group.id"),getPartitions())
    if(errOrConsumerOffsets.isLeft){
      throw new SparkException(
        """
          |Usage:com.daoke360.kafka.KafkaManager
          |message:尝试获取分区消费偏移量失败
        """.stripMargin
      )
    }
    errOrConsumerOffsets.right.get
  }
  //保存rdd的消费偏移量
  private val offsetRangesAtomicReference = new AtomicReference[Array[OffsetRange]]()



  /**
    * 创建输入的dstream
    */
  def createDirectDstream[
  K: ClassTag,
  V: ClassTag,
  KD <: Decoder[K] : ClassTag,
  VD <: Decoder[V] : ClassTag,
  R: ClassTag
  ](ssc:StreamingContext)={
    //取出保存在第三方的消费偏移量
    val fromOffsets = getConsumeOffsets()
    //打印日志（起始消费位置）
    printFromOffsets(fromOffsets)
    //创建输入的dstream
    val kafkaInputDstream = KafkaUtils.createDirectStream[K, V, KD, VD, R](
      ssc,
      kafkaParams,
      fromOffsets,
      (messageAndMetData: MessageAndMetadata[K, V]) => {
        messageAndMetData.message().asInstanceOf[R]
      }
    ).transform(rdd => {
      //rdd==>kafkaRdd
      //获取当前rdd的消费偏移量
      val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      offsetRangesAtomicReference.set(offsetRanges)
      rdd
    })
    kafkaInputDstream

  }
  /**
    * 更新消费偏移量
    */
  def updateConsumeOffsets() = {
    val offsetRanges: Array[OffsetRange] = offsetRangesAtomicReference.get()
    println("************************************************")
    offsetRanges.foreach(offsetRange =>{
      //当前分区
      val topicAndPartition = offsetRange.topicAndPartition()
      //结束的消费位置
      val untilOffset = offsetRange.untilOffset
      val offset = Map[TopicAndPartition,Long](topicAndPartition ->untilOffset)
      println(s"正在更新消费偏移量【topic:${topicAndPartition.topic}   partition:${topicAndPartition.partition}    untilOffset:${offset}】")
      //更新当前分区
      kafkaCluster.setConsumerOffsets(kafkaParams("group.id"),offset)
    })
    println("**************************************************")

  }
  /**
    * 获取分区信息
    *
    * @return
    */
  private def getPartitions(): Set[TopicAndPartition]= {
    //尝试获取分区元数据信息
    val errOrPartitions = kafkaCluster.getPartitions(topics)
    //判断是否获取到了
    if(errOrPartitions.isLeft){
      throw new SparkException(
      """
        |Usage:com.daoke360.kafka.KafkaManager
        |message:尝试获取分区元数据失败
      """.stripMargin)
    }
    //取出分区元数据信息
    errOrPartitions.right.get
  }
  /**
    * 获取每个分区最早消息偏移量
    */
  private def getEarliestLeaderOffsets(topicAndPartitions: Set[TopicAndPartition]) = {
    //通过分区信息,尝试获取每个分区最早消息偏移量
    val errOrlatestLeaderOffsets = kafkaCluster.getEarliestLeaderOffsets(topicAndPartitions)
    if(errOrlatestLeaderOffsets.isLeft){
      throw new SparkException(
        """
          |Usage:com.daoke360.kafka.KafkaManager
          |message:尝试获取每个分区最新消息偏移量失败
        """.stripMargin)
    }
    errOrlatestLeaderOffsets.right.get
  }
  /**
    * 获取每个分区最新消息偏移量
    */
  private def getLatestLeaderOffsets(topicAndPartitions: Set[TopicAndPartition]) = {
    //通过分区信息，尝试获取每个分区最早消息偏移量
    val errOrlatestLeaderOffsets = kafkaCluster.getLatestLeaderOffsets(topicAndPartitions)
    if(errOrlatestLeaderOffsets.isLeft){
      throw new SparkException(
        """
          |Usage:com.daoke360.kafka.KafkaManager
          |message:尝试获取每个分区最新消息偏移量失败
        """.stripMargin)
    }
    errOrlatestLeaderOffsets.right.get
  }
  def printFromOffsets(fromOffsets: Map[TopicAndPartition, Long]) = {
    println("==========================================================")
    fromOffsets.foreach(t2 =>{
      println(s"【topic:${t2._1.topic}   partition:${t2._1.partition}    fromOffset:${t2._2}】")
    })
    println("=============================================================")
  }
}
