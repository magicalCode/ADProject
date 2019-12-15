package com.log

import java.util.Properties

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}


object par2MySQL {

  def main(args: Array[String]): Unit = {

     val spark: SparkSession = SparkSession
       .builder().appName("sql")
       .master("local")
       .getOrCreate()

    if (args.length != 2){
      sys.exit()
    }

    // 获取数据
    val Array(inputPath,outputPath) = args
    val df: DataFrame = spark.read.parquet(inputPath)
    df.createTempView("log")
    val result: DataFrame = spark.sql("select provincename,cityname,count(*) cnt from log group by provincename,cityname")
    //存本地json
//    result.write.partitionBy("provincename","cityname").json(outputPath)

    // 配置mysql属性
    val load: Config = ConfigFactory.load()
    val prop = new Properties()
    prop.setProperty("user",load.getString("jdbc_username"))
    prop.setProperty("password",load.getString("jdbc_password"))
    //存mysql
    result.write.mode(SaveMode.Append).jdbc(load.getString("jdbc_url"),"procity",prop);

    spark.stop()

  }

}
