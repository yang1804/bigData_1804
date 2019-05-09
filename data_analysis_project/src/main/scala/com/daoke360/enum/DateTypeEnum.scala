package com.daoke360.enum

/**
  * Created by 华硕电脑 on 2019/4/29.
  */
object DateTypeEnum extends Enumeration{
  val YEAR = Value(0, "year")
  val SEASON = Value(1, "season")
  val MONTH = Value(2, "month")
  val WEEK = Value(3, "week")
  val DAY = Value(4, "day")
}
