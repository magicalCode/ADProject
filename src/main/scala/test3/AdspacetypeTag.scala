package test3

import com.alibaba.fastjson.JSONObject

object AdspacetypeTag extends Test3Tags {

  override def makeTags(args: Any*): List[(String, Double)] = {

    var list = List[(String,Double)]()

    val json = args(0).asInstanceOf[JSONObject]
    val AD: String = args(1).toString

    val adspacetype: String = json.getString("adspacetype")

    list:+=(AD+"@"+adspacetype,1.0)

    list
  }
}
