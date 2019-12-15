package com.tags

import com.util.Tags
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.Row

/**
  * 关键字标签
  */
object TagsKeyWords extends Tags{
  override def makeTags(args: Any*): List[(String, Int)] = {

    var list =List[(String,Int)]()
    // 类型转换
    val row = args(0).asInstanceOf[Row]

    val stopwords = args(1).asInstanceOf[Broadcast[Array[String]]]

    // 获取数据
    val kw = row.getAs[String]("keywords").split("\\|")
    // 过滤
    kw.filter(str=>str.length>=3&&str.length<=8 && !stopwords.value.contains(str))
      .foreach(t=>list:+=("K"+t,1))

    //返回
    list
  }
}
