package com.daoke360.task.etl

import com.daoke360.bean.caseclass.IPRule
import com.daoke360.common.AnalysisLog
import com.daoke360.config.ConfigurationManager
import com.daoke360.constants.{GlobalConstants, LogConstants}
import com.daoke360.task.BaseTask
import com.daoke360.util.Utils
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.SparkException
import org.apache.spark.rdd.RDD

import scala.collection.mutable

/**
  * Created by 华硕电脑 on 2019/4/26.
  */
object AnalysisLogTask extends BaseTask{
var inputDate: String = null
  var inputPath: String = null
  //输入记录数的累加器
  val inputRecordAccumulator = sc.longAccumulator("inputRecordAccumulator")
  //过滤记录数累加器
  val filterRecordAccumulator = sc.longAccumulator("filterRecordAccumulator")
  /**
    * 验证参数是否正确
    * 1,验证参数的个数  >=1
    * 2,验证参数的各是 yyyy-MM-dd
    * @param args
    */
  private def validateInputArgs(args: Array[String]) = {
    if(args.length == 0){
      throw new SparkException(
        """
          |Usage:com.daoke360.task.etl.AnalysisLogTask
          |errorMessage:任务至少需要有一个日期参数
        """.stripMargin)
    }
    if(!Utils.validateInputDate(args(0))){
      throw new SparkException(
        """
          |Usage:com.daoke360.task.etl.AnalysisLogTask
          |errorMessage:任务第一个参数是一个日期，日期的格式是：yyyy-MM-dd
        """.stripMargin)
    }
    inputDate=args(0)
  }
  /**
    * 2,验证当天是否存在用户行为日志
    * /logs/2019/04/24/xx.log
    * 2019-04-24===Long类型时间戳===>2019/04/24==>/logs/logs/2019/04/24==>验证这个路径在hdfs上是否存在
    */
  private def validateExistsLog():Unit = {
     inputPath =
    ConfigurationManager.getValue(GlobalConstants.CONFIG_LOG_PATH_PREFIX) +
      Utils.formatDate(Utils.parseDate(inputDate, "yyyy-MM-dd"), "yyyy/MM/dd")
    var fileSystem: FileSystem = null
    try{
      fileSystem=FileSystem.newInstance(configuration)
      if(!fileSystem.exists(new Path(inputPath))){
        throw new SparkException(
          s"""
             |Usage:com.daoke360.task.etl.AnalysisLogTask
             |errorMessage:指定的日期${inputDate},不存在需要解析的用户行为日志
         """.stripMargin
        )
      }
    } catch {
      case e: Exception =>e.printStackTrace()
    }finally {
      if(fileSystem !=null)
        fileSystem.close()
    }

  }

  private def loadIPRule() = {
    val ipRuleArray: Array[IPRule] =
      sc.textFile(ConfigurationManager.getValue(GlobalConstants.CONFIG_IP_RULE_DATA_PATH),2)
      .map(line =>{
        val fields = line.split("[|]")
        IPRule(fields(2).toLong,fields(3).toLong,fields(5),fields(6),
          fields(7))
      }).collect()
    ipRuleArray
  }

 private def loadLogFromHdfs(ipRuleArray: Array[IPRule]) = {
   val broadcast = sc.broadcast(ipRuleArray)
   val logRDD = sc.textFile(inputPath,4).map(logText =>{
     inputRecordAccumulator.add(1)
     AnalysisLog.analysisLog(logText,broadcast.value)
   }).filter(x =>{
     if(x != null){
       true
     }else {
       filterRecordAccumulator.add(1)
       false
     }
   })
   logRDD
 }

  private def saveLogToHbase(logRDD: RDD[mutable.Map[String, String]]):Unit = {
    val jobConf = new JobConf(configuration)
    jobConf.addResource("hbase-site.xml")
    jobConf.setOutputFormat(classOf[TableOutputFormat])
    jobConf.set(TableOutputFormat.OUTPUT_TABLE,LogConstants.HBASE_LOG_TABLE_NAME)
    logRDD.map(map =>{
      val accessTime = map(LogConstants.LOG_COLUMNS_NAME_ACCESS_TIME)
      val uid = map(LogConstants.LOG_COLUMNS_NAME_UID)
      val eventName = map(LogConstants.LOG_COLUMNS_NAME_EVENT_NAME)
      val rowKey = accessTime + "_" + Math.abs((uid + eventName).hashCode)
      val put = new Put(rowKey.getBytes())
      map.foreach(t2 => {
        val key = t2._1
        val value = t2._2
        put.addColumn(LogConstants.HBASE_LOG_TABLE_FAMILY.getBytes(), key.getBytes(), value.getBytes())
      })
      (new ImmutableBytesWritable(), put)
    }).saveAsHadoopDataset(jobConf)
  }

  def main(args: Array[String]): Unit = {
    //1.验证参数是否成功
    validateInputArgs(args)

    //2.验证当天是否存在用户行为日志
    validateExistsLog()
    //3.使用spark加载ip规则
    val ipRuleArray = loadIPRule()
    //4.使用spark加载用户行为日志，进行解析
    val logRDD = loadLogFromHdfs(ipRuleArray)
    //5.将解析好的日志，保存到hbase上
   saveLogToHbase(logRDD)
    println(s"本次输入日志记录数：${inputRecordAccumulator.value}条，过滤日志记录数：${filterRecordAccumulator.value}条")
    sc.stop()
  }
}
