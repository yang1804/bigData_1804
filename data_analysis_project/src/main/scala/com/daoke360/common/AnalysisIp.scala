package com.daoke360.common

import com.daoke360.bean.caseclass.{IPRule, RegionInfo}
import com.daoke360.util.Utils
import scala.util.control.Breaks._

/**
  * Created by 华硕电脑 on 2019/4/25.
  */
object AnalysisIp {
def getRegionInfoByIp(ip: String,ipRuleArray:Array[IPRule])={
  val regionInfo = RegionInfo()
  val numIp = Utils.ipToLong(ip)
val index = binarySearch(numIp,ipRuleArray)
  if(index != -1){
    val ipRule = ipRuleArray(index)
    regionInfo.country = ipRule.country
    regionInfo.province = ipRule.province
    regionInfo.city = ipRule.city
  }
  regionInfo
}
  def binarySearch(numIp:Long,ipRuleArray:Array[IPRule])={
    var index = -1
    var min=0
    var max=ipRuleArray.length - 1
    breakable({
      while (min<=max){
        val middle = (min+max)/2
        val iPRule = ipRuleArray(middle)
        if(numIp>=iPRule.startIP && numIp<= iPRule.endIP){
          index=middle
          break()
        }else if(numIp>iPRule.endIP){
          min=middle+1
        }else if(numIp<iPRule.startIP){
          max=middle-1
        }
      }
    })
    index
  }
}
