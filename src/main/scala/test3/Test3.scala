package test3

import com.alibaba.fastjson.{JSON, JSONObject}
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object Test3 {

  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder()
      .appName("tags").master("local")
      .getOrCreate()

    val Array(dataInputPath,dicapp,dicdevice) = args

    val lines: RDD[String] = spark.sparkContext.textFile(dataInputPath)

    val jsonRdd: RDD[JSONObject] = lines.map(x => {
      JSON.parseObject(x)
    })

    val load: Config = ConfigFactory.load()
    val AD: String = load.getString("adspacetype")
    val CH: String = load.getString("channelid")
    val AI: String = load.getString("appid")
    val AM: String = load.getString("appName")
    val SX: String = load.getString("sex")
    val PN: String = load.getString("province")
    val CN: String = load.getString("city")
    val KW: String = load.getString("keywords")
    val AG: String = load.getString("age")

    jsonRdd.map(json => {

      // 用户id
      val userId: String = json.getString("userid")
      //广告类型
       val adTag: List[(String, Double)] = AdspacetypeTag.makeTags(json,AD)
      // 渠道
      val chTag: List[(String, Double)] = Channeltag.makeTags(json,CH)
     // App名称
      val appnameTag: List[(String, Double)] = AppNameTag.makeTags(json,AM)
      //关键词
      val kwTag: List[(String, Double)] = KeywordsTag.makeTags(json,KW)
      //设备
      val deviceTag: List[(String, Double)] = DeviceTag.makeTags(json)
      //性别 年龄
      val sexTag: List[(String, Double)] = SexTag.makeTags(json,SX)

      val TagList = adTag++chTag++appnameTag++kwTag++deviceTag++sexTag

      (userId,TagList)

    }).foreach(println)


    jsonRdd.map(json => {
      //性别 年龄
      val sexTag: List[(String, Double)] = SexTag.makeTags(json,SX)
      (0,sexTag)
    }).reduceByKey((list1,list2)=> {
      (list1 ++ list2).groupBy(_._1).map {
        case (k, sexTag) => (k, sexTag.map(_._2).sum)
      }.toList
    }).foreach(println)

  }

}
