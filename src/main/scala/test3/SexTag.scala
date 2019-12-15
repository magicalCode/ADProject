package test3

import com.alibaba.fastjson.JSONObject

object SexTag extends Test3Tags {
  override def makeTags(args: Any*): List[(String, Double)] = {

    var list = List[(String,Double)]()

    val json = args(0).asInstanceOf[JSONObject]
    val SX: String = args(1).toString

    val sex: String = json.getString("sex")
    val age: Int = json.getString("age").toInt


    if (sex.equals("0")){
      age match {
        case v if v<=20 => list:+=("男"+"AG小于20",1.0)
        case v if v<=30 => list:+=("男"+"AG21-30",1.0)
        case v if v<=40 => list:+=("男"+"AG31-40",1.0)
        case v if v>40 => list:+=("男"+"AG41以上",1.0)
      }

    }else{
      age match {
        case v if v<=20 => list:+=("女"+"AG小于20",1.0)
        case v if v<=30 => list:+=("女"+"AG21-30",1.0)
        case v if v<=40 => list:+=("女"+"AG31-40",1.0)
        case v if v>40 => list:+=("女"+"AG41以上",1.0)
      }
    }

    list
  }
}
