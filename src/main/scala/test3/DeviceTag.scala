package test3

import com.alibaba.fastjson.JSONObject

object DeviceTag extends Test3Tags {
  override def makeTags(args: Any*): List[(String, Double)] = {

    var list = List[(String,Double)]()
    val json = args(0).asInstanceOf[JSONObject]

    val client: Int = json.getString("client").toInt
    client match {
      case 1 =>list:+=("D00010001",1.0)
      case 2 =>list:+=("D00010002",1.0)
      case 3 =>list:+=("D00010003",1.0)
      case 4 =>list:+=("D00010004",1.0)
    }

    val network: String = json.getString("networkmannername")
    network match {
      case "WIFI" =>list:+=("D00020001",1.0)
      case "4G" =>list:+=("D00020002",1.0)
      case "3G" =>list:+=("D00020003",1.0)
      case "2G" =>list:+=("D00020004",1.0)
      case _ =>list:+=("D00020005",1.0)
    }

    val ispname: String = json.getString("ispname")
    ispname match {
      case "移动" =>list:+=("D00030001",1.0)
      case "联通" =>list:+=("D00030002",1.0)
      case "电信" =>list:+=("D00030003",1.0)
      case _ =>list:+=("D00030004",1.0)
    }

    list
  }
}
