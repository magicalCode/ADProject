package test3

import com.alibaba.fastjson.JSONObject

object AppNameTag extends Test3Tags {
  override def makeTags(args: Any*): List[(String, Double)] = {

    var list = List[(String,Double)]()

    val json = args(0).asInstanceOf[JSONObject]
    val AM: String = args(1).toString

    val appname: String = json.getString("appname")

    list:+=(AM+"@"+appname,1.0)

    list

  }
}
