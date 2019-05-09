package com.daoke360.dao

import com.daoke360.bean.domain.{AreaTop3Product, StatsUser}
import com.daoke360.jdbc.JdbcHelper

/**
  * Created by 华硕电脑 on 2019/5/7.
  */
object AreaTop3ProductDao {
  def insertBatch(areaTop3ProductArray: Array[AreaTop3Product]) = {
    val sql = "insert into area_top3_product values(?,?,?,?,?)"
    val sqlParamsArray = new Array[Array[Any]](areaTop3ProductArray.length)
    for (i <- 0 until (areaTop3ProductArray.length)){
      val areaTop3Product = areaTop3ProductArray(i)
      sqlParamsArray(i)=Array[Any](
        areaTop3Product.date_dimension_id,
        areaTop3Product.location_dimension_id,
        areaTop3Product.product_id,
        areaTop3Product.browser_product_count,
        areaTop3Product.city_infos
      )
    }
    JdbcHelper.executeBatch(sql,sqlParamsArray)
  }

  def deleteByDateDimensionId(dateDimensionId: Int) = {
    val sql="delete from area_top3_product where date_dimension_id=?"
    val sqlParams = Array[Any](dateDimensionId)
    JdbcHelper.executeUpdate(sql,sqlParams)
  }
}
