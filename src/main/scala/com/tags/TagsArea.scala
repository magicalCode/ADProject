package com.tags

import com.util.Tags
import org.apache.spark.sql.Row

/**
  * 地域标签
  */
object TagsArea extends Tags{
  override def makeTags(args: Any*): List[(String, Int)] = {

    var list =List[(String,Int)]()
    val row = args(0).asInstanceOf[Row]
    // 获取地域信息
    val pro = row.getAs[String]("provincename")
    val city = row.getAs[String]("cityname")
    if(!pro.isEmpty){
      list:+=("ZP"+pro,1)
    }
    if(!city.isEmpty){
      list:+=("ZC"+city,1)
    }

    //返回
    list
  }
}
