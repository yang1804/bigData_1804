package com.daoke360.task

import com.daoke360.util.Utils
import org.apache.hadoop.conf.Configuration
import org.apache.spark.{SparkConf, SparkException}
import org.apache.spark.sql.SparkSession

/**
  * Created by 华硕电脑 on 2019/4/26.
  */
trait BaseTask {
 val configuration = new Configuration()
  configuration.addResource("hbase-site.xml")
   val sparkConf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local")
 sparkConf.set("spark.sql.shuffle.partitions","1")
   val spark = SparkSession.builder().config(sparkConf).getOrCreate()
   val sc = spark.sparkContext
 var inputDate: String = null
 def validateInputArgs(args: Array[String]) = {
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
}
