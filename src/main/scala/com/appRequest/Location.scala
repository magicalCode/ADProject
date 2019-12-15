package com.appRequest

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * 地域指标统计
  */
object Location {

  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession
      .builder()
      .appName("location")
      .master("local")
      .getOrCreate()

    if (args.length != 2){
      sys.exit()
    }

    // 获取数据
    val Array(inputPath,outputPath) = args
    val df: DataFrame = spark.read.parquet(inputPath)
    df.createTempView("log")

    // 地域分布
//    areaDistrution(spark)

    // 终端设备 运营商
//    carrieroperator(spark)

    // 网络类型
//    networkType(spark)

    // 设备类型
//    devicetype(spark)

    // 操作系统
//    client(spark)

    // 媒体类别
    appFrom(spark)

  }

  // 地域分布
  def areaDistrution(spark:SparkSession)= {
    spark.sql(
      """
        |select
        |provincename,cityname,
        |sum(case when requestmode = 1 and processnode >=1 then 1 else 0 end) ysrequest,
        |sum(case when requestmode = 1 and processnode >=2 then 1 else 0 end) yxrequest,
        |sum(case when requestmode = 1 and processnode >=3 then 1 else 0 end) adrequest,
        |sum(case when iseffective = 1 and isbilling = 1 and isbid = 1 then 1 else 0 end) cybid,
        |sum(case when iseffective = 1 and isbilling = 1 and iswin = 1 and adorderid != 0 then 1 else 0 end) cybidsuccess,
        |sum(case when requestmode = 2 and iseffective = 1 then 1 else 0 end) show,
        |sum(case when requestmode = 3 and iseffective = 1 then 1 else 0 end) click,
        |sum(case when iseffective = 1 and isbilling = 1 and iswin = 1 then winprice/1000 else 0 end) dspWinPrice,
        |sum(case when iseffective = 1 and isbilling = 1 and iswin = 1 then adpayment/1000 else 0 end) dspadpayment
        |from
        |log
        |group by
        |provincename,cityname
        |
      """.stripMargin).show()
  }

  // 终端设备 运营商
  def carrieroperator(spark:SparkSession)= {
    spark.sql(
      """
        |select
        |ispid,ispname,
        |sum(case when requestmode = 1 and processnode >=1 then 1 else 0 end) ysrequest,
        |sum(case when requestmode = 1 and processnode >=2 then 1 else 0 end) yxrequest,
        |sum(case when requestmode = 1 and processnode >=3 then 1 else 0 end) adrequest,
        |sum(case when iseffective = 1 and isbilling = 1 and isbid = 1 then 1 else 0 end) cybid,
        |sum(case when iseffective = 1 and isbilling = 1 and iswin = 1 and adorderid != 0 then 1 else 0 end) cybidsuccess,
        |sum(case when requestmode = 2 and iseffective = 1 then 1 else 0 end) show,
        |sum(case when requestmode = 3 and iseffective = 1 then 1 else 0 end) click,
        |sum(case when iseffective = 1 and isbilling = 1 and iswin = 1 then winprice/1000 else 0 end) dspWinPrice,
        |sum(case when iseffective = 1 and isbilling = 1 and iswin = 1 then adpayment/1000 else 0 end) dspadpayment
        |from
        |log
        |group by
        |ispid,ispname
        |order by ispid
        |
      """.stripMargin).show()
  }

  // 网络类型
  def networkType(spark:SparkSession)={
    spark.sql(
      """
        |select
        |networkmannerid,networkmannername,
        |sum(case when requestmode = 1 and processnode >=1 then 1 else 0 end) ysrequest,
        |sum(case when requestmode = 1 and processnode >=2 then 1 else 0 end) yxrequest,
        |sum(case when requestmode = 1 and processnode >=3 then 1 else 0 end) adrequest,
        |sum(case when iseffective = 1 and isbilling = 1 and isbid = 1 then 1 else 0 end) cybid,
        |sum(case when iseffective = 1 and isbilling = 1 and iswin = 1 and adorderid != 0 then 1 else 0 end) cybidsuccess,
        |sum(case when requestmode = 2 and iseffective = 1 then 1 else 0 end) show,
        |sum(case when requestmode = 3 and iseffective = 1 then 1 else 0 end) click,
        |sum(case when iseffective = 1 and isbilling = 1 and iswin = 1 then winprice/1000 else 0 end) dspWinPrice,
        |sum(case when iseffective = 1 and isbilling = 1 and iswin = 1 then adpayment/1000 else 0 end) dspadpayment
        |from
        |log
        |group by
        |networkmannerid,networkmannername
        |order by networkmannerid
      """.stripMargin).show()
  }

  // 设备类型
  def devicetype(spark:SparkSession)={

    spark.sql(
      """
        |select
        |devicetype,
        |sum(case when requestmode = 1 and processnode >=1 then 1 else 0 end) ysrequest,
        |sum(case when requestmode = 1 and processnode >=2 then 1 else 0 end) yxrequest,
        |sum(case when requestmode = 1 and processnode >=3 then 1 else 0 end) adrequest,
        |sum(case when iseffective = 1 and isbilling = 1 and isbid = 1 then 1 else 0 end) cybid,
        |sum(case when iseffective = 1 and isbilling = 1 and iswin = 1 and adorderid != 0 then 1 else 0 end) cybidsuccess,
        |sum(case when requestmode = 2 and iseffective = 1 then 1 else 0 end) show,
        |sum(case when requestmode = 3 and iseffective = 1 then 1 else 0 end) click,
        |sum(case when iseffective = 1 and isbilling = 1 and iswin = 1 then winprice/1000 else 0 end) dspWinPrice,
        |sum(case when iseffective = 1 and isbilling = 1 and iswin = 1 then adpayment/1000 else 0 end) dspadpayment
        |from
        |log
        |group by
        |devicetype
        |order by devicetype
      """.stripMargin).show()
  }

  // 操作系统
  def client(spark:SparkSession)={

    spark.sql(
      """
        |select
        |client,device,
        |sum(case when requestmode = 1 and processnode >=1 then 1 else 0 end) ysrequest,
        |sum(case when requestmode = 1 and processnode >=2 then 1 else 0 end) yxrequest,
        |sum(case when requestmode = 1 and processnode >=3 then 1 else 0 end) adrequest,
        |sum(case when iseffective = 1 and isbilling = 1 and isbid = 1 then 1 else 0 end) cybid,
        |sum(case when iseffective = 1 and isbilling = 1 and iswin = 1 and adorderid != 0 then 1 else 0 end) cybidsuccess,
        |sum(case when requestmode = 2 and iseffective = 1 then 1 else 0 end) show,
        |sum(case when requestmode = 3 and iseffective = 1 then 1 else 0 end) click,
        |sum(case when iseffective = 1 and isbilling = 1 and iswin = 1 then winprice/1000 else 0 end) dspWinPrice,
        |sum(case when iseffective = 1 and isbilling = 1 and iswin = 1 then adpayment/1000 else 0 end) dspadpayment
        |from
        |log
        |group by
        |client,device
        |
      """.stripMargin).show()
  }

  // 媒体类别
  def appFrom(spark:SparkSession)={

    spark.sql(
      """
        |select
        |appid,appname,
        |sum(case when requestmode = 1 and processnode >=1 then 1 else 0 end) ysrequest,
        |sum(case when requestmode = 1 and processnode >=2 then 1 else 0 end) yxrequest,
        |sum(case when requestmode = 1 and processnode >=3 then 1 else 0 end) adrequest,
        |sum(case when iseffective = 1 and isbilling = 1 and isbid = 1 then 1 else 0 end) cybid,
        |sum(case when iseffective = 1 and isbilling = 1 and iswin = 1 and adorderid != 0 then 1 else 0 end) cybidsuccess,
        |sum(case when requestmode = 2 and iseffective = 1 then 1 else 0 end) show,
        |sum(case when requestmode = 3 and iseffective = 1 then 1 else 0 end) click,
        |sum(case when iseffective = 1 and isbilling = 1 and iswin = 1 then winprice/1000 else 0 end) dspWinPrice,
        |sum(case when iseffective = 1 and isbilling = 1 and iswin = 1 then adpayment/1000 else 0 end) dspadpayment
        |from
        |log
        |group by
        |appid,appname
        |
      """.stripMargin).show()

  }



}
