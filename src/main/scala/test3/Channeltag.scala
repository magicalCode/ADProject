package test3

import com.alibaba.fastjson.JSONObject

object Channeltag  extends Test3Tags {
  override def makeTags(args: Any*): List[(String, Double)] = {

    var list = List[(String,Double)]()

    val json = args(0).asInstanceOf[JSONObject]
    val CH: String = args(1).toString

    val channelid: String = json.getString("channelid")

    list:+=(CH+"@"+channelid,1.0)

    list
  }
}
