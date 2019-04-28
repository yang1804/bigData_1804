package com.daoke360.task

import org.apache.hadoop.conf.Configuration
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * Created by 华硕电脑 on 2019/4/26.
  */
trait BaseTask {
 val configuration = new Configuration()
  configuration.addResource("hbase-site.xml")
   val sparkConf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local")
   val spark = SparkSession.builder().config(sparkConf).getOrCreate()
   val sc = spark.sparkContext
}
