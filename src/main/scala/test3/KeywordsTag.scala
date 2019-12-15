package test3

import com.alibaba.fastjson.JSONObject

object KeywordsTag extends Test3Tags {
  override def makeTags(args: Any*): List[(String, Double)] = {

    var list = List[(String,Double)]()

    val json = args(0).asInstanceOf[JSONObject]
    val KW: String = args(1).toString

    val keywords: Array[String] = json.getString("keywords").split(",")

    for (words <- keywords) {
      list:+=(KW+"@"+words,1.0)
    }
    list
  }
}
