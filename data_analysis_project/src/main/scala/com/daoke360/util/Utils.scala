package com.daoke360.util

import java.text.SimpleDateFormat
import java.util.Date
import java.util.regex.Pattern

/**
  * Created by 华硕电脑 on 2019/4/25.
  */
object Utils {
  //格式化日期
  def formatDate(longTime: Long, s: String) = {
    val sdf = new SimpleDateFormat(s)
    sdf.format(new Date(longTime))
  }

  //将日期转换成时间戳
  def parseDate(inputDate: String, s: String) = {
    val sdf = new SimpleDateFormat(s)
    sdf.parse(inputDate).getTime
  }

  def validateInputDate(s: String) = {
    val reg = "^(?:(?!0000)[0-9]{4}-(?:(?:0[1-9]|1[0-2])-(?:0[1-9]|1[0-9]|2[0-8])|(?:0[13-9]|1[0-2])-(?:29|30)|(?:0[13578]|1[02])-31)|(?:[0-9]{2}(?:0[48]|[2468][048]|[13579][26])|(?:0[48]|[2468][048]|[13579][26])00)-02-29)$"
    Pattern.compile(reg).matcher(s).matches()
  }

  def ipToLong(ip:String)={
  var numIp: Long=0
  val items = ip.split("[.]")
  for(item <-items){
    numIp=(numIp << 8 | item.toLong)
  }
  numIp
}
}
