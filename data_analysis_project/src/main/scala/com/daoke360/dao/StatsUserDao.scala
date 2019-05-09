package com.daoke360.dao

import com.daoke360.bean.domain.StatsUser
import com.daoke360.jdbc.JdbcHelper

/**
  * Created by 华硕电脑 on 2019/5/5.
  */
object StatsUserDao {
  def insertBatch(statsUserArray: Array[StatsUser]) = {
    val sql = "insert into stats_user values(?,?,?,?,?,?,?)"
    val sqlParamsArray = new Array[Array[Any]](statsUserArray.length)
    for (i <- 0 until (statsUserArray.length)){
      val statsUser = statsUserArray(i)
      sqlParamsArray(i)=Array[Any](
        statsUser.date_dimension_id,
        statsUser.platform_dimension_id,
        statsUser.active_users,
        statsUser.new_install_users,
        statsUser.session_count,
        statsUser.session_length,
        statsUser.created
      )
    }
    JdbcHelper.executeBatch(sql,sqlParamsArray)
  }

  def deleteByDateDimensionId(date_dimension_id: Int) = {
    val sql="delete from stats_user where date_dimension_id=?"
    val sqlParams = Array[Any](date_dimension_id)
    JdbcHelper.executeUpdate(sql,sqlParams)
  }

}
