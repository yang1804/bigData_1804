package com.daoke360.task.analysis

import com.daoke360.bean.domain.{AreaTop3Product, SessionAggrStat, StatsDeviceLocation, StatsUser}
import com.daoke360.bean.domain.dimension.{DateDimension, LocationDimension, PlatformDimension}
import com.daoke360.constants.{GlobalConstants, LogConstants}
import com.daoke360.dao._
import com.daoke360.enum.EventEnum
import com.daoke360.jdbc.JdbcHelper
import com.daoke360.task.BaseTask
import com.daoke360.util.Utils
import org.apache.commons.lang.StringUtils
import org.apache.hadoop.hbase.client.{Result, Scan}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.util.{Base64, Bytes}
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StringType, StructField, StructType}

import scala.collection.mutable

/**
  * Created by 华硕电脑 on 2019/4/28.
  */
object SessionAnalysisTask extends BaseTask{

  /**
    * 从hbase中加载指定日期当前的所有日志
    */
  def loadDataFromHbase() = {
    val startDateTime = Utils.parseDate(inputDate,"yyyy-MM-dd").toString
    val endDateTime = Utils.getNextDate(startDateTime.toLong).toString
    val scan = new Scan()
    //设置开始扫描位置
    scan.setStartRow(startDateTime.getBytes())
    //设置结束的扫描位置
    scan.setStopRow(endDateTime.getBytes())
    val protoScan = ProtobufUtil.toScan(scan)
    //使用base64算法对protoscan进行编码，编码成字符串
    val base64StringScan = Base64.encodeBytes(protoScan.toByteArray)

    val jobConf = new JobConf(configuration)
    //设置需要加载的表
    jobConf.set(TableInputFormat.INPUT_TABLE,LogConstants.HBASE_LOG_TABLE_NAME)
    //设置扫描器
    jobConf.set(TableInputFormat.SCAN,base64StringScan)

    val resultRDD = sc.newAPIHadoopRDD(jobConf, classOf[TableInputFormat], classOf[ImmutableBytesWritable],
      classOf[Result]).map(_._2)
    val eventLogRDD = resultRDD.map(result =>{
      val uid = Bytes.toString(result.getValue(LogConstants.HBASE_LOG_TABLE_FAMILY.getBytes, LogConstants.LOG_COLUMNS_NAME_UID.getBytes()))
      val sid = Bytes.toString(result.getValue(LogConstants.HBASE_LOG_TABLE_FAMILY.getBytes, LogConstants.LOG_COLUMNS_NAME_SID.getBytes()))
      val accessTime = Bytes.toString(result.getValue(LogConstants.HBASE_LOG_TABLE_FAMILY.getBytes, LogConstants.LOG_COLUMNS_NAME_ACCESS_TIME.getBytes()))
      val eventName = Bytes.toString(result.getValue(LogConstants.HBASE_LOG_TABLE_FAMILY.getBytes, LogConstants.LOG_COLUMNS_NAME_EVENT_NAME.getBytes()))
      val country = Bytes.toString(result.getValue(LogConstants.HBASE_LOG_TABLE_FAMILY.getBytes, LogConstants.LOG_COLUMNS_NAME_COUNTRY.getBytes()))
      val province = Bytes.toString(result.getValue(LogConstants.HBASE_LOG_TABLE_FAMILY.getBytes, LogConstants.LOG_COLUMNS_NAME_PROVINCE.getBytes()))
      val city = Bytes.toString(result.getValue(LogConstants.HBASE_LOG_TABLE_FAMILY.getBytes, LogConstants.LOG_COLUMNS_NAME_CITY.getBytes()))
      val platform = Bytes.toString(result.getValue(LogConstants.HBASE_LOG_TABLE_FAMILY.getBytes, LogConstants.LOG_COLUMNS_NAME_PLATFORM.getBytes()))
      val browserName = Bytes.toString(result.getValue(LogConstants.HBASE_LOG_TABLE_FAMILY.getBytes, LogConstants.LOG_COLUMNS_NAME_BROWSER_NAME.getBytes()))
      val productId = Bytes.toString(result.getValue(LogConstants.HBASE_LOG_TABLE_FAMILY.getBytes, LogConstants.LOG_COLUMNS_NAME_PRODUCT_ID.getBytes()))
      val osName = Bytes.toString(result.getValue(LogConstants.HBASE_LOG_TABLE_FAMILY.getBytes, LogConstants.LOG_COLUMNS_NAME_OS_NAME.getBytes()))
      (uid, sid, accessTime, eventName, country, province, city, platform, browserName, productId, osName)
    })
    eventLogRDD
    //platform sid uid eventName

  }
//  def daypert(eventLogRDD: RDD[(String, String, String, String, String, String, String, String, String, String, String)]) = {
//    var xingSet:Set[String]=null
//    var houSet:Set[String]=null
//    var sessSet:Set[String]=null
//    val tuple4RDD = eventLogRDD.map(t11 =>(t11._1,t11._2,t11._4,t11._8))//uid sid eventName platform
//    val tuple2RDD = tuple4RDD.map(t4 => {
//      (t4._4, t4)
//    })
//      val tuple2RDDe = tuple2RDD.groupByKey()
//    tuple2RDDe.collect().map(t2 =>{
//      xingSet=  Set[String]()
//      houSet=  Set[String]()
//      sessSet=  Set[String]()
//      t2._2.map(t4 =>{
//        sessSet.+=(t4._2)
//        if(t4._4.equals("e_l")){
//          xingSet.+=(t4._1)
//        }else{
//          houSet.+=(t4._1)
//        }
//      })
//      (t2._1,xingSet.size,houSet.size,sessSet.size)
//    }).foreach(println(_))
//        val filter: RDD[(String, (String, String, String, String))] = tuple2RDD.filter(t4 => {
//          t4._2._4.equals("e_l")
//        })
//    filter.groupByKey().map(line =>{
//      val size = line._2.size
//      (line._1,size)
//    })
//val rdd01: RDD[(String, (String, String, String))] = dataFromHbase.flatMap(fun => {
//  List(
//    (date.value + "-" + fun._8, ("uid", fun._1, date.value + "-" + fun._8)),
//    (date.value + "-" + fun._8, ("sid", fun._2, date.value + "-" + fun._8)),
//    (date.value + "-" + fun._8, ("eName", fun._4, date.value + "-" + fun._8))
//  )
//})
//    val filterRdd: RDD[(String, (String, String, String))] = rdd01.filter(tuple => {
//      if (tuple._2._1.equals("eName")) {
//        if (tuple._2._2.equals("e_l")) {
//          true
//        } else {
//          false
//        }
//      } else {
//        true
//      }
//    })
//    val rdd02: RDD[((String, String, String), Int)] = filterRdd.map(tuple => {
//      (tuple._2, 1)
//    })
//    rdd02.reduceByKey(_ + _).map(tuple => {
//      if (tuple._1._1.equals("eName")) {
//        (tuple._1._3 + "-新用户", tuple._2)
//      } else if (tuple._1._1.equals("uid")) {
//        (tuple._1._3 + "-活跃用户", 1)
//      } else {
//        (tuple._1._3 + "-会话数量", 1)
//      }
//    }).reduceByKey(_ + _).foreach(println(_))

//    tuple2RDDe.map(t2 =>{
//
//      var xing =0;
//      var sids=0;
//      var hou=0;
//      t2._2.map(t4 =>{
//        if(t4._2.equals("e_l")){
//          xing +=1
//        }else {
//          hou+= 1
//        }
//      })
//      val by1: Map[String, Iterable[(String, String, String, String)]] = t2._2.groupBy(_._2)
//      sids=by1.size
//
//(t2._1,(hou,xing,sids))
//    })
// }


  def sessionVisitTimeAndStepLengthAnalysisStat(eventLogRDD: RDD[(String, String, String, String, String, String, String, String, String, String, String)]) = {
    //取出需要的字段
    //tuple4RDD==>( sid, accessTime, eventName,platform)
    val tuple4RDD = eventLogRDD.map(t11 =>(t11._2,t11._3,t11._4,t11._8))
    //以时间accessTime和platform平台作为key，以sid, accessTime, eventName,platform作为value
    val tuple2RDD = tuple4RDD.map(t4 => {
      ((Utils.formatDate(t4._2.toLong, "yyyy-MM-dd"), t4._4), t4)
    })
    val flatMapRDD = tuple2RDD.flatMap(t2 => {
      Array(
        //所有的平台
        ((t2._1._1, GlobalConstants.VALUE_OF_ALL), t2._2),
        //具体的平台
        t2
      )
    })
    //将同一天同一个平台的数据聚合在一起
    /**
      * ((accessTime,platform),List(( sid, accessTime, eventName,platform),( sid, accessTime, eventName,platform),...))
      */
    val groupRDD = flatMapRDD.groupByKey()
    /**
      * 计算出同一天同一个平台中，每个session的访问时长和访问步长
      * ((accessTime,platform),List((sid,visitTimeLength,visitStepLength),(sid,visitTimeLength,visitStepLength),..))
      */
    val sessionTimeAndStepLengthRDD=groupRDD.map(t2 => {
      //t2==> ((accessTime,platform),List(( sid, accessTime, eventName,platform),( sid, accessTime, eventName,platform),...))
      //按会话id进行分组(sid,List(( sid, accessTime, eventName,platform),( sid, accessTime, eventName,platform)))
      val it = t2._2.groupBy(_._1).map(g => {
        //g==>(sid,List(( sid, accessTime, eventName,platform),( sid, accessTime, eventName,platform)))
        var visitStepLength,visitTimeLength:Long = 0L
        var startTime, endTime: Long=0L
        g._2.foreach(t4 =>{
          val eventName = t4._3
          if(eventName.equals(EventEnum.PAGE_VIEW_EVENT.toString) ||
          eventName.equals(EventEnum.BROWSER_PRODUCT_EVENT.toString)){
            visitStepLength +=1
          }
          val accessTime = t4._2.toLong
          if(startTime == 0 || accessTime<startTime) startTime=accessTime
          if (endTime == 0 || accessTime > endTime) endTime = accessTime
        })
        visitTimeLength=(endTime - startTime) / 1000
        (g._1,visitTimeLength,visitStepLength)
      })
      //((accessTime,platform),List((sid,visitTimeLength,visitStepLength),(sid,visitTimeLength,visitStepLength),..))
      (t2._1,it)
    })
    /**
      * 判断同一天同一个平台，每个session访问时长和步长所属区间，对应区间+1
      * "session_count=0|1s_3s=1|4s_6s=0|....."
      */
    val sessionTimeAndStepLengthRangeRDD = sessionTimeAndStepLengthRDD.map(t2 =>{
      //t2==>((accessTime,platform),List((sid,visitTimeLength,visitStepLength),(sid,visitTimeLength,visitStepLength),..))
      //session_count=0|1s_3s=0|4s_6s=0|7s_9s=0|10s_30s=0|30s_60s=0|1m_3m=0|3m_10m=0|10m_30m=0|30m=0|1_3=0|4_6=0|7_9=0|10_30=0|30_60=0|60=0
      val stringAccumulator = new StringAccumulator()

      t2._2.foreach(t3 =>{
        stringAccumulator.add(GlobalConstants.SESSION_COUNT)
        val visitTimeLength = t3._2
        val visitStepLength = t3._3
        if (visitTimeLength >= 0 && visitTimeLength <= 3) {
          stringAccumulator.add(GlobalConstants.TIME_1s_3s)
        } else if (visitTimeLength >= 4 && visitTimeLength <= 6) {
          stringAccumulator.add(GlobalConstants.TIME_4s_6s)
        } else if (visitTimeLength >= 7 && visitTimeLength <= 9) {
          stringAccumulator.add(GlobalConstants.TIME_7s_9s)
        } else if (visitTimeLength >= 10 && visitTimeLength <= 30) {
          stringAccumulator.add(GlobalConstants.TIME_10s_30s)
        } else if (visitTimeLength > 30 && visitTimeLength <= 60) {
          stringAccumulator.add(GlobalConstants.TIME_30s_60s)
        } else if (visitTimeLength > 1 * 60 && visitTimeLength <= 3 * 60) {
          stringAccumulator.add(GlobalConstants.TIME_1m_3m)
        } else if (visitTimeLength > 3 * 60 && visitTimeLength <= 10 * 60) {
          stringAccumulator.add(GlobalConstants.TIME_3m_10m)
        } else if (visitTimeLength > 10 * 60 && visitTimeLength <= 30 * 60) {
          stringAccumulator.add(GlobalConstants.TIME_10m_30m)
        } else if (visitTimeLength > 30 * 60) {
          stringAccumulator.add(GlobalConstants.TIME_30m)
        }

        if (visitStepLength >= 1 && visitStepLength <= 3) {
          stringAccumulator.add(GlobalConstants.STEP_1_3)
        } else if (visitStepLength >= 4 && visitStepLength <= 6) {
          stringAccumulator.add(GlobalConstants.STEP_4_6)
        } else if (visitStepLength >= 7 && visitStepLength <= 9) {
          stringAccumulator.add(GlobalConstants.STEP_7_9)
        } else if (visitStepLength >= 10 && visitStepLength <= 30) {
          stringAccumulator.add(GlobalConstants.STEP_10_30)
        } else if (visitStepLength > 30 && visitStepLength <= 60) {
          stringAccumulator.add(GlobalConstants.STEP_30_60)
        } else if (visitStepLength > 60) {
          stringAccumulator.add(GlobalConstants.STEP_60)
        }
      })
      (t2._1,stringAccumulator.value)
    })
    /**
      * ((2019-04-25,ios),session_count=646|1s_3s=555|4s_6s=1|7s_9s=0|10s_30s=8|30s_60s=11|1m_3m=25|3m_10m=46|10m_30m=0|30m=0|1_3=646|4_6=0|7_9=0|10_30=0|30_60=0|60=0)
      * ((2019-04-25,pc),session_count=1025|1s_3s=577|4s_6s=1|7s_9s=3|10s_30s=7|30s_60s=23|1m_3m=83|3m_10m=331|10m_30m=0|30m=0|1_3=890|4_6=72|7_9=29|10_30=33|30_60=1|60=0)
      * ((2019-04-25,all),session_count=1713|1s_3s=1134|4s_6s=2|7s_9s=5|10s_30s=15|30s_60s=37|1m_3m=116|3m_10m=404|10m_30m=0|30m=0|1_3=1576|4_6=74|7_9=29|10_30=33|30_60=1|60=0)
      * ((2019-04-25,android),session_count=42|1s_3s=2|4s_6s=0|7s_9s=2|10s_30s=0|30s_60s=3|1m_3m=8|3m_10m=27|10m_30m=0|30m=0|1_3=40|4_6=2|7_9=0|10_30=0|30_60=0|60=0)
      *
      * SessionAggrStat
      * SessionAggrStat
      * SessionAggrStat
      * SessionAggrStat
      *
      **/
    val connection = JdbcHelper.getConnection()
    val sessionAggrStatArray =
      sessionTimeAndStepLengthRangeRDD.collect().map(t2 =>{
      val sessionAggrStat = new SessionAggrStat()
      val accessTime = t2._1._1
      val platform = t2._1._2
      sessionAggrStat.date_dimension_id=DimensionDao.getDimensionId(DateDimension.buildDateDimension(accessTime),connection)
      sessionAggrStat.platform_dimension_id = DimensionDao.getDimensionId(new PlatformDimension(0,platform),connection)

      sessionAggrStat.session_count=Utils.getFieldValue(t2._2,GlobalConstants.SESSION_COUNT).toInt
      sessionAggrStat.time_1s_3s = Utils.getScale(Utils.getFieldValue(t2._2,GlobalConstants.TIME_1s_3s).toDouble / sessionAggrStat.session_count,2)
      sessionAggrStat.time_4s_6s = Utils.getScale(Utils.getFieldValue(t2._2, GlobalConstants.TIME_4s_6s).toDouble / sessionAggrStat.session_count, 2)
      sessionAggrStat.time_7s_9s = Utils.getScale(Utils.getFieldValue(t2._2, GlobalConstants.TIME_7s_9s).toDouble / sessionAggrStat.session_count, 2)
      sessionAggrStat.time_10s_30s = Utils.getScale(Utils.getFieldValue(t2._2, GlobalConstants.TIME_10s_30s).toDouble / sessionAggrStat.session_count, 2)
      sessionAggrStat.time_30s_60s = Utils.getScale(Utils.getFieldValue(t2._2, GlobalConstants.TIME_30s_60s).toDouble / sessionAggrStat.session_count, 2)
      sessionAggrStat.time_1m_3m = Utils.getScale(Utils.getFieldValue(t2._2, GlobalConstants.TIME_1m_3m).toDouble / sessionAggrStat.session_count, 2)
      sessionAggrStat.time_3m_10m = Utils.getScale(Utils.getFieldValue(t2._2, GlobalConstants.TIME_3m_10m).toDouble / sessionAggrStat.session_count, 2)
      sessionAggrStat.time_10m_30m = Utils.getScale(Utils.getFieldValue(t2._2, GlobalConstants.TIME_10m_30m).toDouble / sessionAggrStat.session_count, 2)
      sessionAggrStat.time_30m = Utils.getScale(Utils.getFieldValue(t2._2, GlobalConstants.TIME_30m).toDouble / sessionAggrStat.session_count, 2)
      sessionAggrStat.step_1_3 = Utils.getScale(Utils.getFieldValue(t2._2, GlobalConstants.STEP_1_3).toDouble / sessionAggrStat.session_count, 2)
      sessionAggrStat.step_4_6 = Utils.getScale(Utils.getFieldValue(t2._2, GlobalConstants.STEP_4_6).toDouble / sessionAggrStat.session_count, 2)
      sessionAggrStat.step_7_9 = Utils.getScale(Utils.getFieldValue(t2._2, GlobalConstants.STEP_7_9).toDouble / sessionAggrStat.session_count, 2)
      sessionAggrStat.step_10_30 = Utils.getScale(Utils.getFieldValue(t2._2, GlobalConstants.STEP_10_30).toDouble / sessionAggrStat.session_count, 2)
      sessionAggrStat.step_30_60 = Utils.getScale(Utils.getFieldValue(t2._2, GlobalConstants.STEP_30_60).toDouble / sessionAggrStat.session_count, 2)
      sessionAggrStat.step_60 = Utils.getScale(Utils.getFieldValue(t2._2, GlobalConstants.STEP_60).toDouble / sessionAggrStat.session_count, 2)
      sessionAggrStat
    })
    if(connection !=null)
      connection.close()
    //将sessionAggrStatArray保持到mysql中
   SessionAggrStatDao.deleteByDateDimensionId(sessionAggrStatArray(0).date_dimension_id)
    SessionAggrStatDao.insertBatch(sessionAggrStatArray)

  }

//(uid, sid, accessTime, eventName, country, province, city, platform, browserName, productId, osName)
  def userStats(eventLogRDD: RDD[(String, String, String, String, String, String, String, String, String, String, String)]) = {
    val tuple5Rdd = eventLogRDD.map(t11=>(t11._1,t11._2,t11._3,t11._4,t11._8))
    val tuple2Rdd = tuple5Rdd.map(t5 => {
      ((Utils.formatDate(t5._3.toLong, "yyyy-MM-dd"), t5._5), (t5._1, t5._2, t5._4))
    })
    //区分具体平台和所有平台
    val flatMapRdd = tuple2Rdd.flatMap(t2 => {
      Array(
        ((t2._1._1, GlobalConstants.VALUE_OF_ALL), t2._2),
        t2
      )
    })
    //按时间为平台维度进行聚合((accessTime,platform),List((uid,sid,eventName),(uid,sid,eventName),(uid,sid,eventName),...))
    val groupRdd = flatMapRdd.groupByKey()
    val tuple2Array = groupRdd.map(t2 => {
      var newUserCount: Int = 0
      var pageViewCount: Int = 0
      val uidSet = mutable.Set[String]()
      val sidSet = mutable.Set[String]()
      t2._2.foreach(t3 => {
        val uid = t3._1
        val sid = t3._2
        val eventName = t3._3
        uidSet.add(uid)
        sidSet.add(sid)
        if (eventName.equals(EventEnum.LAUNCH_EVENT.toString)) newUserCount += 1
        if (eventName.equals(EventEnum.PAGE_VIEW_EVENT.toString) || eventName.equals(EventEnum.BROWSER_PRODUCT_EVENT.toString)) pageViewCount += 1
      })
      val active_users = uidSet.size
      val session_count = sidSet.size
      val new_install_users = newUserCount
      val session_length = pageViewCount / session_count

      (t2._1, (active_users, session_count, new_install_users, session_length))
    }).collect()
    val connection = JdbcHelper.getConnection()
    val statsUserArray = tuple2Array.map(t2 => {
      val date_dimension_id = DimensionDao.getDimensionId(DateDimension.buildDateDimension(t2._1._1), connection)
      val platform_dimension_id = DimensionDao.getDimensionId(new PlatformDimension(0, t2._1._2), connection)
      val active_users = t2._2._1
      val new_install_users = t2._2._3
      val session_count = t2._2._2
      val session_length = t2._2._4
      val created = t2._1._1
      new StatsUser(date_dimension_id, platform_dimension_id, active_users, new_install_users, session_count, session_length, created)
    })
    StatsUserDao.deleteByDateDimensionId(statsUserArray(0).date_dimension_id)
    StatsUserDao.insertBatch(statsUserArray)
  }
  //(uid, sid, accessTime, eventName, country, province, city, platform, browserName, productId, osName)
  def userSessionStat(eventLogRDD: RDD[(String, String, String, String, String, String, String, String, String, String, String)]) = {
    val tuple7Rdd = eventLogRDD.map(t11 =>(t11._1,t11._2,t11._3,t11._5,t11._6,t11._7,t11._8))
    val tuple2Rdd = tuple7Rdd.map(t7 => {
      ((Utils.formatDate(t7._3.toLong, "yyyy-MM-dd"), t7._7, t7._4, t7._5, t7._6), (t7._1, t7._2))
    })
    val flatMapRdd = tuple2Rdd.flatMap(t2 => {
      Array(
        ((t2._1._1, GlobalConstants.VALUE_OF_ALL, t2._1._3, t2._1._4, t2._1._5), t2._2),
        ((t2._1._1, GlobalConstants.VALUE_OF_ALL, t2._1._3,t2._1._4 , GlobalConstants.VALUE_OF_ALL), t2._2),
        ((t2._1._1, GlobalConstants.VALUE_OF_ALL, t2._1._3, GlobalConstants.VALUE_OF_ALL, GlobalConstants.VALUE_OF_ALL), t2._2),
        t2,
        ((t2._1._1, t2._1._2, t2._1._3, t2._1._4, GlobalConstants.VALUE_OF_ALL), t2._2),
        ((t2._1._1, t2._1._2, t2._1._3, GlobalConstants.VALUE_OF_ALL, GlobalConstants.VALUE_OF_ALL), t2._2)
      )
    })
    val groupRdd = flatMapRdd.groupByKey()
//    val groupRdd = tuple2Rdd.groupByKey()
//    val arrays = groupRdd.map(t2 => {
//      val uidSet = mutable.Set[String]()
//      val sidSet = mutable.Set[String]()
//      var tairCount: Int = 0
//      val sidRdd: Map[String, Iterable[(String, String, String)]] = t2._2.groupBy(_._2)
//      sidRdd.foreach(ts2 => {
//        val sid = ts2._1
//        sidSet.add(sid)
//        if (ts2._2.size == 1) tairCount += 1
//        ts2._2.foreach(t3 => {
//          val uid = t3._1
//          uidSet.add(uid)
//        })
//      })
//      (t2._1, uidSet.size, sidSet.size, tairCount)
//    }).collect()
val arrays = groupRdd.map(g => {
  val uidSet = mutable.Set[String]()
  val sidMap = mutable.Map[String, Int]()
  g._2.foreach(t2 => {
    val uid = t2._1
    val sid = t2._2
    uidSet.add(uid)
    sidMap.put(sid, sidMap.getOrElse(sid, 0) + 1)
  })
  val active_users = uidSet.size
  val session_count = sidMap.size
  val bounce_sessions = sidMap.filter(x => x._2 == 1).size
  (g._1, active_users, session_count, bounce_sessions)
}).collect()
    val connection = JdbcHelper.getConnection()
    val statsDeviceLocationArray = arrays.map(t4 => {
      val date_dimension_id = DimensionDao.getDimensionId(DateDimension.buildDateDimension(t4._1._1), connection)
      val platform_dimension_id = DimensionDao.getDimensionId(new PlatformDimension(0, t4._1._2), connection)
      val location_dimension_id = DimensionDao.getDimensionId(new LocationDimension(0, t4._1._3, t4._1._4, t4._1._5), connection)
      val active_users = t4._2
      val session_count = t4._3
      val bounce_sessions = t4._4
      val created = t4._1._1
      new StatsDeviceLocation(date_dimension_id,
        platform_dimension_id, location_dimension_id, active_users, session_count, bounce_sessions, created)
    })

    if(connection != null) connection.close()
    //将数据保存到mysql表中stats_device_location
    StatsDeviceLocationDao.deleteByDateDimensionId(statsDeviceLocationArray(0).date_dimension_id)
    StatsDeviceLocationDao.insertBatch(statsDeviceLocationArray)
  }

  def eventStat(eventLogRDD: RDD[(String, String, String, String, String, String, String, String, String, String, String)]) = {
    val tuple4Rdd = eventLogRDD.map(t11 =>(t11._1,t11._3,t11._4,t11._8))
    val tuple2Rdd: RDD[((String, String, String), String)] = tuple4Rdd.map(t4 => {
      ((Utils.formatDate(t4._2.toLong,"yyyy-MM-dd"), t4._4, t4._3), t4._1)
    })
    val ArrayRdd = tuple2Rdd.flatMap(t2 => {
      Array(
        ((t2._1._1, t2._1._2, GlobalConstants.VALUE_OF_ALL), t2._2),
        ((t2._1._1, GlobalConstants.VALUE_OF_ALL, t2._1._3), t2._2),
        ((t2._1._1, GlobalConstants.VALUE_OF_ALL, GlobalConstants.VALUE_OF_ALL), t2._2),
        t2
      )
    })
    val groupRdd = ArrayRdd.groupByKey()
    groupRdd.map(t2=>{
      (t2._1,t2._2.size)
    }).collect().foreach(println(_))
  }

  def areaBrowserProductTop3Stats(eventLogRDD: RDD[(String, String, String, String, String, String, String, String, String, String, String)]) = {
    val rowRdd = eventLogRDD.filter(x => {
      x._4.equals(EventEnum.BROWSER_PRODUCT_EVENT.toString) && StringUtils.isNotBlank(x._10)
    }).map(x => Row(Utils.formatDate(x._3.toLong, "yyyy-MM-yy"), x._5, x._6, x._7, x._10))
    /**
      * rdd转换成dataframe有两种方式
      * dataframe=rdd+schema
      * 这个rdd里面的数据类型必须是一个行对象row
      * 1，通过反射推断每一列的列名和列的数据类型
      * 2，自定义元数据（指定了列名和列的数据类型）
      */
    val schema=StructType(
      List(
        StructField("date",StringType,false),
        StructField("country",StringType,false),
        StructField("province",StringType,false),
        StructField("city",StringType,false),
        StructField("product_id",StringType,false)
      )
    )
    spark.createDataFrame(rowRdd,schema).createOrReplaceTempView("area_browser_product_view")
    spark.sql(
      """
        |select date,country,province,city,product_id,count(product_id) browser_count
        |from area_browser_product_view
        |group by date,country,province,city,product_id

      """.stripMargin
    ).createOrReplaceTempView("area_browser_product_count_view")
    //注册用户自定义聚合函数
    spark.udf.register("city_concat_func", new CityConcatUDAF)
    //spark sql 默认在shuffle read阶段有200个分区,由于我们的数量比较小，所以不需要这么多分区
    val df = spark.sql(
      """
select date,country,province,product_id,browser_count,city_infos
from(
      select row_number()over(partition by date,country,province order by  browser_count desc)rank,date,country,province,product_id,browser_count,city_infos
  from(
    select date,country,province,product_id,sum(browser_count) browser_count,city_concat_func(city) city_infos
    from area_browser_product_count_view
    group by date,country,province,product_id
  )temp
)tmp
where tmp.rank<=3
      """.stripMargin)
    val connection = JdbcHelper.getConnection()
    val areaTop3ProductArray = df.collect().map(row => {
      val date_dimension_id: Int = DimensionDao.getDimensionId(DateDimension.buildDateDimension(row.getAs[String]("date")), connection)
      val location_dimension_id: Int = DimensionDao.getDimensionId(new LocationDimension(0, row.getAs[String]("country"), row.getAs[String]("province"), GlobalConstants.VALUE_OF_ALL), connection)
      val product_id = row.getAs[String]("product_id").toLong
      val browser_product_count = row.getAs[Long]("browser_count")
      val city_infos = row.getAs[String]("city_infos")
      new AreaTop3Product(date_dimension_id, location_dimension_id, product_id, browser_product_count, city_infos)
    })
    if(connection != null)connection.close()
    AreaTop3ProductDao.deleteByDateDimensionId(areaTop3ProductArray(0).date_dimension_id)
    AreaTop3ProductDao.insertBatch(areaTop3ProductArray)

  }

  def main(args: Array[String]): Unit = {
    validateInputArgs(args)
    val eventLogRDD = loadDataFromHbase()
//   sessionVisitTimeAndStepLengthAnalysisStat(eventLogRDD)
   // userStats(eventLogRDD)
   // userSessionStat(eventLogRDD)
    //统计同一天同一个平台每个事件发生的总次数
    //eventStat(eventLogRDD)
    //统计同一天同一地区浏览次数排名前3的商品
    areaBrowserProductTop3Stats(eventLogRDD)
  }
}
