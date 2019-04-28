package com.daoke360.config

import org.apache.hadoop.conf.Configuration

/**
  * Created by 华硕电脑 on 2019/4/26.
  */
object ConfigurationManager {
private val configuration = new Configuration()
  configuration.addResource("project-config.xml")
  def getValue(key: String) = {
    configuration.get(key)
  }
}
