package com.tags

import com.util.Tags
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.Row

/**
  * App名称标签
  */
object TagsAppName extends Tags{
  override def makeTags(args: Any*): List[(String, Int)] = {

    var list = List[(String,Int)]()

    // 类型转换
    val row = args(0).asInstanceOf[Row]
    //获取广播变量
    val appMap = args(1).asInstanceOf[Broadcast[collection.Map[String, String]]]

    // 获取appid，appname
    val appname = row.getAs[String]("appname")
    val appid = row.getAs[String]("appid")

    if(!appname.isEmpty){
      list:+=("APP"+appname,1)
    }else if(!appid.isEmpty){
      list:+=("APP"+appMap.value.getOrElse(appid,"其他"),1)
    }

    //返回
    list
  }
}
