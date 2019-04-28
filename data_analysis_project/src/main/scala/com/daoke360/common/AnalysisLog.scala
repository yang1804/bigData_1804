package com.daoke360.common

import java.net.URLDecoder

import com.daoke360.bean.caseclass.IPRule
import com.daoke360.constants.LogConstants
import org.apache.commons.lang.StringUtils

import scala.collection.mutable


/**
  * Created by 华硕电脑 on 2019/4/25.
  */
object AnalysisLog {

  def handleIP(logMap: mutable.Map[String, String], ipRuleArray: Array[IPRule]) = {
    val ip = logMap(LogConstants.LOG_COLUMNS_NAME_IP)
    val regionInfo = AnalysisIp.getRegionInfoByIp(ip, ipRuleArray)
    logMap.put(LogConstants.LOG_COLUMNS_NAME_COUNTRY, regionInfo.country)
    logMap.put(LogConstants.LOG_COLUMNS_NAME_PROVINCE, regionInfo.province)
    logMap.put(LogConstants.LOG_COLUMNS_NAME_CITY, regionInfo.city)
  }

  def hadleRequestParams(logMap: mutable.Map[String, String], s: String) = {
    val fields = s.split("[?]")
    if(fields.length == 2 && StringUtils.isNotBlank(fields(1))){
      val paramsText = fields(1)
      val items = paramsText.split("[&]")
      for (item <- items){
        val kv = item.split("[=]")
        if(kv.length == 2 && StringUtils.isNotBlank(kv(0)) && StringUtils.isNotBlank(kv(1))){
          val key = URLDecoder.decode(kv(0),"utf-8")
          val value = URLDecoder.decode(kv(1),"utf-8")
          logMap.put(key,value)
        }
      }
    }
  }

  def analysisLog(logText:String, ipRuleArray:Array[IPRule])={
  var logMap: mutable.Map[String,String]=null
  if(StringUtils.isNotBlank(logText)){
    val fields = logText.split("[|]")
    if(fields.length == 4){
       logMap = mutable.Map[String,String]()
      logMap.put(LogConstants.LOG_COLUMNS_NAME_IP,fields(0))
      logMap.put(LogConstants.LOG_COLUMNS_NAME_ACCESS_TIME, fields(1))
      handleIP(logMap,ipRuleArray)
      hadleRequestParams(logMap,fields(3))
    }


  }
    logMap
}
//  def main(args: Array[String]): Unit = {
//    println(analysisLog("123.150.182.176|1556095899547|119.23.63.188:81|/log.gif?en=e_pv&ver=1&pl=android&os_n=linux&b_n=chrome&b_v=57.0.2987.108&sdk=js&uid=C9C801EE-A3DB-4B43-BC9F-944908981B99&sid=638C7CD0-7448-4721-B6AE-4285913A0556&b_rst=360*760&b_usa=Mozilla%2F5.0%20(Linux%3B%20U%3B%20Android%208.1.0%3B%20zh-CN%3B%20COL-AL10%20Build%2FHUAWEICOL-AL10)%20AppleWebKit%2F537.36%20(KHTML%2C%20like%20Gecko)%20Version%2F4.0%20Chrome%2F57.0.2987.108%20UCBrowser%2F12.1.6.996%20Mobile%20Safari%2F537.36&l=zh-Hans-CN&ct=1539827195598&tt=%E9%A6%96%E9%A1%B5-%E5%88%80%E5%AE%A2%E7%A8%8B%E5%BA%8F%E5%91%98-%E5%81%9A%E7%BA%BF%E4%B8%8A%E8%89%AF%E5%BF%83%E5%A4%A7%E6%95%B0%E6%8D%AE%E5%9F%B9%E8%AE%AD&url=http%3A%2F%2Fwww.daoke360.com%2F",null))
//  }
}
