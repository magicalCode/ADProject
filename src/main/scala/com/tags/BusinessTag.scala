package com.tags

import ch.hsr.geohash.GeoHash
import com.util.{AmapUtil, JedisConn, StrUtils, Tags}
import org.apache.spark.sql.Row

object BusinessTag extends Tags{

  override def makeTags(args: Any*): List[(String, Int)] = {

    var list =List[(String,Int)]()
    val row = args(0).asInstanceOf[Row]
    // 获取经纬度
    val long = row.getAs[String]("long")
    val lat = row.getAs[String]("lat")
    if(StrUtils.toDouble(lat)==0.0 && StrUtils.toDouble(long)==0.0){
      // 通过经纬度获取商圈
      val business = getBusiness(long,lat)
      val lines = business.split(",")
      lines.foreach(t=>{
        list:+=(t,1)
      })
    }

    list
  }

  /**
    * 获取商圈
    * @param long
    * @param lat
    * @return
    */
  def getBusiness(long: String, lat: String): String = {
    val geoHash = GeoHash.geoHashStringWithCharacterPrecision(StrUtils.toDouble(lat),StrUtils.toDouble(long),6)
    // 去查询数据库
    var str = redis_queryBusiness(geoHash)
    // 查询高德
    if(str==null||str.length==0){
      str = AmapUtil.getBusinessFromAMap(StrUtils.toDouble(long),StrUtils.toDouble(lat))
      // 存储到redis
      redis_insertBusiness(geoHash,str)
    }
    str
  }

  /**
    * 查询数据库
    * @param geoHash
    * @return
    */
  def redis_queryBusiness(geoHash: String): String = {
    val jedis = JedisConn.getConn()
    val str = jedis.get(geoHash)
    jedis.close()
    str
  }

  /**
    * 将数据存储redis
    * @param geoHash
    * @param str
    * @return
    */
  def redis_insertBusiness(geoHash: String, str: String) = {
    val jedis = JedisConn.getConn()
    jedis.set(geoHash,str)
    jedis.close()
  }

}
