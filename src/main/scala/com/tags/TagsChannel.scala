package com.tags

import com.util.Tags
import org.apache.spark.sql.Row

/**
  * 渠道标签
  */
object TagsChannel extends Tags{
  override def makeTags(args: Any*): List[(String, Int)] = {

    var list =List[(String,Int)]()
    val row = args(0).asInstanceOf[Row]

    // 渠道标签
    val adp = row.getAs[Int]("adplatformproviderid")
    if(adp.toString!=null && !adp.equals(0)){
      list:+=("CN"+adp,1)
    }
    //返回
    list
  }
}
