package com.daoke360.task.streaming

import com.daoke360.bean.caseclass.IPRule
import com.daoke360.bean.domain.StatsLocationFlow
import com.daoke360.bean.domain.dimension.{DateDimension, LocationDimension}
import com.daoke360.common.AnalysisLog
import com.daoke360.config.ConfigurationManager
import com.daoke360.constants.{GlobalConstants, LogConstants}
import com.daoke360.dao.{DimensionDao, StatsLocationFlowDao}
import com.daoke360.enum.EventEnum
import com.daoke360.jdbc.JdbcHelper
import com.daoke360.kafka.KafkaManager
import com.daoke360.task.etl.AnalysisLogTask.sc
import com.daoke360.util.Utils
import kafka.serializer.StringDecoder
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
  * Created by 华硕电脑 on 2019/5/9.
  */
object FlowTask {
  private def loadIPRule(sc: SparkContext) = {
    val ipRuleArray: Array[IPRule] =
      sc.textFile(ConfigurationManager.getValue(GlobalConstants.CONFIG_IP_RULE_DATA_PATH),2)
        .map(line =>{
          val fields = line.split("[|]")
          IPRule(fields(2).toLong,fields(3).toLong,fields(5),fields(6),
            fields(7))
        }).collect()
    ipRuleArray
  }

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[*]")
    val sc = new SparkContext(sparkConf)
    sc.setLogLevel("warn")
    val ssc = new StreamingContext(sc,Seconds(2))
    ssc.checkpoint("/checkpoint/b1804")
    //加载ip规则
    val ipRules = loadIPRule(sc)
    //广播ip规则
    val ipRulesBroadcast = sc.broadcast(ipRules)
    val kafkaParams = Map[String, String](
      //kafka broker地址
      "metadata.broker.list" -> "hadoopk1:9092,hadoopk2:9092",
      "group.id" -> "b1804",
      "auto.offset.reset" -> "largest"
    )
    val kafkaManager: KafkaManager = new KafkaManager(kafkaParams, Set("event_log"))
    val kafkaInputDstream: DStream[String] = kafkaManager.createDirectDstream[String, String, StringDecoder, StringDecoder, String](ssc)

    val beitDstream: DStream[((String, String, String, String), Array[Any])] = kafkaInputDstream.map(line => {
      AnalysisLog.analysisLog(line, ipRulesBroadcast.value)
    }).filter(x => x != null).flatMap(m => {
      val accessTime = Utils.formatDate(m(LogConstants.LOG_COLUMNS_NAME_ACCESS_TIME).toLong, "yyyy-MM-dd")
      val country = m(LogConstants.LOG_COLUMNS_NAME_COUNTRY)
      val province = m(LogConstants.LOG_COLUMNS_NAME_PROVINCE)
      val city = m(LogConstants.LOG_COLUMNS_NAME_CITY)
      val eventName = m(LogConstants.LOG_COLUMNS_NAME_EVENT_NAME)
      val uid = m(LogConstants.LOG_COLUMNS_NAME_UID)
      val sid = m(LogConstants.LOG_COLUMNS_NAME_SID)
      Array(
        ((accessTime, country, GlobalConstants.VALUE_OF_ALL, GlobalConstants.VALUE_OF_ALL), (eventName, uid, sid)),
        //省
        ((accessTime, country, province, GlobalConstants.VALUE_OF_ALL), (eventName, uid, sid)),
        //市
        ((accessTime, country, province, city), (eventName, uid, sid))
      )
    }).updateStateByKey(
      //实时统计每天每个地区的新增用户数，独立访客，页面浏览次数，会话个数，会话跳出数
      /**
        * 0
        * set
        * 0
        * map
        *
        * Array[Any]
        */
      (it: Iterator[((String, String, String, String), Seq[(String, String, String)], Option[Array[Any]])]) => {
        it.map(t3 => {
          //取出当前key
          val key = t3._1
          //取出当前key在之前聚合的结果
          val array = t3._3.getOrElse(Array[Any](0, mutable.Set[String](), 0, mutable.Map[String, Int]()))
          var nu = array(0).asInstanceOf[Int]
          var uidSet = array(1).asInstanceOf[mutable.Set[String]]
          var pv = array(2).asInstanceOf[Int]
          var sidMap = array(3).asInstanceOf[mutable.Map[String, Int]]

          t3._2.foreach(x3 => {
            //(eventName,uid,sid)
            val eventName = x3._1
            val uid = x3._2
            val sid = x3._3
            if (eventName.equals(EventEnum.LAUNCH_EVENT.toString)) nu += 1
            uidSet.add(uid)
            if (eventName.equals(EventEnum.PAGE_VIEW_EVENT.toString) ||
              eventName.equals(EventEnum.BROWSER_PRODUCT_EVENT.toString)) pv += 1
            sidMap.put(sid, sidMap.getOrElse(sid, 0) + 1)
          })
          array(0) = nu
          array(1) = uidSet
          array(2) = pv
          array(3) = sidMap
          (key, array)
        })
      },
      //分区器
      new HashPartitioner(sc.defaultParallelism),
      //是否记住分区
      true
    )
    beitDstream.foreachRDD(rdd =>{
      //不能再此处创建连接对象，因为此处创建的对象如果在rdd算子中使用，那么就必须进行序列化，而连接对象是不能够被序列化的
      rdd.foreachPartition(partitionIt =>{
        val connection = JdbcHelper.getConnection()
        val arrayBuffer = ArrayBuffer[StatsLocationFlow]()
        partitionIt.foreach(t2 =>{
          //t2==>((accessTime, country, province, city), Array[Any](nu,uidSet,pv,sidMap))
          val array = t2._2
          var date_dimension_id = DimensionDao.getDimensionId(DateDimension.buildDateDimension(t2._1._1),connection)
          var location_dimension_id = DimensionDao.getDimensionId(new LocationDimension(0,t2._1._2,t2._1._3,t2._1._4),connection)
          var nu = (array(0)).asInstanceOf[Int]
          var uv = (array(1).asInstanceOf[mutable.Set[String]]).size
          var pv = (array(2)).asInstanceOf[Int]
          var sn = (array(3).asInstanceOf[mutable.Map[String,Int]]).size
          var on = (array(3).asInstanceOf[mutable.Map[String,Int]]).filter(t2 => t2._2==1).size
          var created: String=t2._1._1
          arrayBuffer.append(new StatsLocationFlow(date_dimension_id,location_dimension_id,
            nu,uv,pv,sn,on,created))
        })
        if(connection != null)
          connection.close()
        if(arrayBuffer.size>0)
          StatsLocationFlowDao.updateBatch(arrayBuffer.toArray)
      })
      kafkaManager.updateConsumeOffsets()
    })

    ssc.start()
    ssc.awaitTermination()
  }
}
