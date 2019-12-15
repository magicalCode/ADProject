package com.tags

import com.typesafe.config.ConfigFactory
import com.util.TagsUtils
import org.apache.hadoop.hbase.{HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.hadoop.hbase.client.{ConnectionFactory, Put}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * 上下文标签
  */
object TagsContext {

  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder()
      .appName("tags").master("local")
      .getOrCreate()
    val Array(inputPath,outputPath,app_dir,stopWord,day)=args

    // Hbase配置
    val load = ConfigFactory.load()
    val hbaseTableName = load.getString("hbase.table.name")
    val configuration = spark.sparkContext.hadoopConfiguration
    configuration.set("hbase.zookeeper.quorum",load.getString("hbase.zookeeper.host"))
    val hbConn = ConnectionFactory.createConnection(configuration)
    println(hbConn)

    val admin = hbConn.getAdmin
    if(!admin.tableExists(TableName.valueOf(hbaseTableName))){
      println("当前表可用")
      // 创建表对象
      val tableDescriptor = new HTableDescriptor(TableName.valueOf(hbaseTableName))
      // 创建列簇
      val columnDescriptor = new HColumnDescriptor("tags")
      // 将列簇加入表中
      tableDescriptor.addFamily(columnDescriptor)
      // 创建表
      admin.createTable(tableDescriptor)
      admin.close()
      hbConn.close()
    }
    // 创建Job
    val jobConf = new JobConf(configuration)
    // 指定key类型
    jobConf.setOutputFormat(classOf[TableOutputFormat])
    // 指定表
    jobConf.set(TableOutputFormat.OUTPUT_TABLE,hbaseTableName)


    //获取数据
    val df: DataFrame = spark.read.parquet(inputPath)

    // 读取字典文件
    val app: RDD[String] = spark.sparkContext.textFile(app_dir)
    val appMap: collection.Map[String, String] = app.filter(_.split("\t").length >= 5).map(t => {
      val str = t.split("\t")
      (str(4), str(1))
    }).collectAsMap()
    //广播
    val broadcastApp: Broadcast[collection.Map[String, String]] = spark.sparkContext.broadcast(appMap)

    // 读取字典文件
    val stopWords: RDD[String] = spark.sparkContext.textFile(stopWord)
    val arr: Array[String] = stopWords.collect()
    //广播
    val broadStopArr: Broadcast[Array[String]] = spark.sparkContext.broadcast(arr)

    //打标签
    df
      //过滤符合的用户数据
      .filter(TagsUtils.oneUserId)
      .rdd
      .map(row=>{

        //拿到UserID
        val userId: String = TagsUtils.getOneUserID(row)

        //广告标签
        val adTag = TagsAD.makeTags(row)

        //App名称标签
        val appNameTag = TagsAppName.makeTags(row,broadcastApp)

        // 渠道
        val channelTag = TagsChannel.makeTags(row)

        //设备
        val adviceTag = TagsClient.makeTags(row)

        //关键字
        val keywordsTag: List[(String, Int)] = TagsKeyWords.makeTags(row,broadStopArr)

        //地域标签
        val areaTag: List[(String, Int)] = TagsArea.makeTags(row)

        // 商圈标签
        val businessTag = BusinessTag.makeTags(row)

        val TagList = adTag++appNameTag++channelTag++adviceTag++keywordsTag++areaTag++businessTag
        (userId,TagList)

      }).reduceByKey((list1,list2)=>{
      list1++list2.groupBy(_._1)
          .mapValues(_.foldLeft[Int](0)(_+_._2))
    }).map{
      case (userId,userTags)=>{
        // 设置rowkey
        val put = new Put(Bytes.toBytes(userId))
        // 添加列的value
        put.addImmutable(Bytes.toBytes("tags"),
          Bytes.toBytes(day),Bytes.toBytes(userTags.mkString(",")))
        // 设置返回对象和put(rowkey，列簇，列的值)
        (new ImmutableBytesWritable(),put)
      }
      // 将数据存储入Hbase
    }.saveAsHadoopDataset(jobConf)

  }
}
