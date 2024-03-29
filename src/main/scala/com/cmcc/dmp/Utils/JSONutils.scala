package com.cmcc.dmp.Utils

import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}

import scala.collection.JavaConversions._

/**
  * requirement 
  *
  * @author zhangsl
  * @date 2019/12/10 9:48 
  * @version 1.0
  */
object JSONutils {

  def  getJson(json:String):String = {
    val buffer = collection.mutable.ListBuffer[String]()
    val jsonOnject: JSONObject = JSON.parseObject(json)
    val status = jsonOnject.getString("status")
    if (status == "0"){
      return null
    }else{
      val regeocode = jsonOnject.getJSONObject("regeocode")
      if (regeocode == null) {
        return null
      }else{
        val addressComponent = regeocode.getJSONObject("addressComponent")
        if (addressComponent == null) {
          return null
        }else{
          val businessAreas = addressComponent.getJSONArray("businessAreas")
          if (businessAreas == null){
            return null
          }else{
            for (i<-businessAreas.toArray){
              if (i.isInstanceOf[JSONObject]){
                val nObject: JSONObject = i.asInstanceOf[JSONObject]
                val name = nObject.getString("name")
                buffer.append(name)
              }
            }
          }
        }
      }
    }
    buffer.mkString(",")
  }

  def main(args: Array[String]): Unit = {
    val json = "{\"status\":\"1\",\"regeocode\":{\"roads\":[{\"id\":\"010J50F0010195642\",\"location\":\"116.31,39.9926\",\"direction\":\"南\",\"name\":\"求知路\",\"distance\":\"67.7634\"},{\"id\":\"010J50F0010195645\",\"location\":\"116.311,39.9919\",\"direction\":\"西\",\"name\":\"五四路\",\"distance\":\"97.0266\"},{\"id\":\"010J50F0010199620\",\"location\":\"116.311,39.9919\",\"direction\":\"西\",\"name\":\"科学路\",\"distance\":\"115.312\"}],\"roadinters\":[{\"second_name\":\"科学路\",\"first_id\":\"010J50F0010195645\",\"second_id\":\"010J50F0010199620\",\"location\":\"116.3112097,39.991065\",\"distance\":\"142.848\",\"first_name\":\"五四路\",\"direction\":\"西北\"}],\"formatted_address\":\"北京市海淀区燕园街道北京大学\",\"addressComponent\":{\"city\":[],\"province\":\"北京市\",\"adcode\":\"110108\",\"district\":\"海淀区\",\"towncode\":\"110108015000\",\"streetNumber\":{\"number\":\"5号\",\"location\":\"116.310454,39.9927339\",\"direction\":\"东北\",\"distance\":\"94.5489\",\"street\":\"颐和园路\"},\"country\":\"中国\",\"township\":\"燕园街道\",\"businessAreas\":[{\"location\":\"116.303364,39.97641\",\"name\":\"万泉河\",\"id\":\"110108\"},{\"location\":\"116.314222,39.98249\",\"name\":\"中关村\",\"id\":\"110108\"},{\"location\":\"116.294214,39.99685\",\"name\":\"西苑\",\"id\":\"110108\"}],\"building\":{\"name\":\"北京大学\",\"type\":\"科教文化服务;学校;高等院校\"},\"neighborhood\":{\"name\":\"北京大学\",\"type\":\"科教文化服务;学校;高等院校\"},\"citycode\":\"010\"},\"aois\":[{\"area\":\"1871566.519603\",\"type\":\"141201\",\"id\":\"B000A816R6\",\"location\":\"116.310905,39.992806\",\"adcode\":\"110108\",\"name\":\"北京大学\",\"distance\":\"0\"}],\"pois\":[{\"id\":\"B000A816R6\",\"direction\":\"东北\",\"businessarea\":\"万泉河\",\"address\":\"颐和园路5号\",\"poiweight\":\"0.806322\",\"name\":\"北京大学\",\"location\":\"116.310905,39.992806\",\"distance\":\"121.741\",\"tel\":\"010-62752114\",\"type\":\"科教文化服务;学校;高等院校\"},{\"id\":\"B000A85J18\",\"direction\":\"东\",\"businessarea\":\"万泉河\",\"address\":\"颐和园路5号北京大学\",\"poiweight\":\"0.17778\",\"name\":\"calis全国文理文献信息中心\",\"location\":\"116.310518,39.991893\",\"distance\":\"44.4465\",\"tel\":[],\"type\":\"科教文化服务;学校;高等院校\"},{\"id\":\"B000A85D1Z\",\"direction\":\"西南\",\"businessarea\":\"万泉河\",\"address\":\"颐和园路5号北京大学附近\",\"poiweight\":\"0.196734\",\"name\":\"中国高校人文社会科学文献中心全国中心\",\"location\":\"116.309910,39.991911\",\"distance\":\"9.42475\",\"tel\":[],\"type\":\"科教文化服务;科教文化场所;科教文化场所\"},{\"id\":\"B0FFFYT6CP\",\"direction\":\"西北\",\"businessarea\":\"万泉河\",\"address\":\"颐和园路5号北京大学\",\"poiweight\":\"0.219387\",\"name\":\"北京大学陈翰生研究中心\",\"location\":\"116.309726,39.992169\",\"distance\":\"33.3487\",\"tel\":[],\"type\":\"科教文化服务;科研机构;科研机构\"},{\"id\":\"B000A192CC\",\"direction\":\"西南\",\"businessarea\":\"万泉河\",\"address\":\"颐和园路5号北京大学\",\"poiweight\":\"0.556491\",\"name\":\"北京大学图书馆\",\"location\":\"116.309972,39.991902\",\"distance\":\"6.66562\",\"tel\":\"010-62757167;010-62751051\",\"type\":\"科教文化服务;图书馆;图书馆\"},{\"id\":\"B000A1FA1F\",\"direction\":\"东南\",\"businessarea\":\"万泉河\",\"address\":\"颐和园路5号\",\"poiweight\":\"0.266845\",\"name\":\"北京大学中国古文献研究中心\",\"location\":\"116.310584,39.990929\",\"distance\":\"124.57\",\"tel\":[],\"type\":\"科教文化服务;科研机构;科研机构\"},{\"id\":\"B0FFG3BFC1\",\"direction\":\"西\",\"businessarea\":\"万泉河\",\"address\":\"颐和园路5号北京大学\",\"poiweight\":\"0.220309\",\"name\":\"北京大数据研究院\",\"location\":\"116.308907,39.992143\",\"distance\":\"95.648\",\"tel\":[],\"type\":\"科教文化服务;科研机构;科研机构|科教文化服务;文化宫;文化宫\"},{\"id\":\"B0FFG3BDN3\",\"direction\":\"西\",\"businessarea\":\"万泉河\",\"address\":\"颐和园路5号北京大学\",\"poiweight\":\"0.220494\",\"name\":\"北京大学前沿计算研究中心\",\"location\":\"116.308769,39.991798\",\"distance\":\"106.618\",\"tel\":[],\"type\":\"科教文化服务;科研机构;科研机构\"},{\"id\":\"B000A9QARU\",\"direction\":\"东\",\"businessarea\":\"万泉河\",\"address\":\"颐和园路5号北京大学地学楼\",\"poiweight\":\"0.2881\",\"name\":\"环境模拟与污染控制国家重点联合实验室(北京大学)\",\"location\":\"116.311877,39.991701\",\"distance\":\"162.16\",\"tel\":[],\"type\":\"科教文化服务;科研机构;科研机构\"},{\"id\":\"B000A7ORS4\",\"direction\":\"东\",\"businessarea\":\"万泉河\",\"address\":\"颐和园路5号北京大学生物楼\",\"poiweight\":\"0.273988\",\"name\":\"北京大学平民学校\",\"location\":\"116.311762,39.992665\",\"distance\":\"169.273\",\"tel\":[],\"type\":\"科教文化服务;学校;学校\"},{\"id\":\"B000A455B0\",\"direction\":\"东\",\"businessarea\":\"万泉河\",\"address\":\"颐和园路5号北京大学地学楼\",\"poiweight\":\"0.26699\",\"name\":\"北京大学中国持续发展研究中心\",\"location\":\"116.311877,39.991701\",\"distance\":\"162.16\",\"tel\":[],\"type\":\"科教文化服务;科研机构;科研机构\"},{\"id\":\"B000A80CVB\",\"direction\":\"东\",\"businessarea\":\"万泉河\",\"address\":\"颐和园路5号北京大学地学楼\",\"poiweight\":\"0.395964\",\"name\":\"北京大学环境与经济研究所\",\"location\":\"116.311877,39.991701\",\"distance\":\"162.16\",\"tel\":[],\"type\":\"科教文化服务;科研机构;科研机构\"},{\"id\":\"B000A80CVA\",\"direction\":\"东\",\"businessarea\":\"万泉河\",\"address\":\"颐和园路5号北京大学地学楼\",\"poiweight\":\"0.131912\",\"name\":\"北京大学环境与健康研究所\",\"location\":\"116.311877,39.991701\",\"distance\":\"162.16\",\"tel\":[],\"type\":\"科教文化服务;科研机构;科研机构\"},{\"id\":\"B000A7ORST\",\"direction\":\"东\",\"businessarea\":\"万泉河\",\"address\":\"颐和园路5号北京大学\",\"poiweight\":\"0.298224\",\"name\":\"北京大学外国哲学研究所\",\"location\":\"116.311895,39.991290\",\"distance\":\"177.427\",\"tel\":\"010-62753319\",\"type\":\"科教文化服务;科研机构;科研机构\"},{\"id\":\"B000A80CVC\",\"direction\":\"东\",\"businessarea\":\"万泉河\",\"address\":\"颐和园路5号北京大学\",\"poiweight\":\"0.225997\",\"name\":\"北京大学汉语语言学研究中心(五四路)\",\"location\":\"116.311895,39.991291\",\"distance\":\"177.376\",\"tel\":[],\"type\":\"科教文化服务;科研机构;科研机构\"},{\"id\":\"B000A9LEA8\",\"direction\":\"北\",\"businessarea\":\"万泉河\",\"address\":\"颐和园路5号北京大学\",\"poiweight\":\"0.216722\",\"name\":\"北京大学陈守仁国际研究中心\",\"location\":\"116.310604,39.993571\",\"distance\":\"186.651\",\"tel\":[],\"type\":\"科教文化服务;科研机构;科研机构\"},{\"id\":\"B000AA11FX\",\"direction\":\"东\",\"businessarea\":\"万泉河\",\"address\":\"颐和园路5号北京大学五四路附近\",\"poiweight\":\"0.188565\",\"name\":\"北京大学妇女性别研究与培训基地\",\"location\":\"116.311885,39.991248\",\"distance\":\"178.662\",\"tel\":[],\"type\":\"科教文化服务;培训机构;培训机构\"},{\"id\":\"B000AA4LVB\",\"direction\":\"东\",\"businessarea\":\"万泉河\",\"address\":\"颐和园路5号北京大学地学楼\",\"poiweight\":\"0.21922\",\"name\":\"北京大学中国环境经济学学科发展项目\",\"location\":\"116.311873,39.991701\",\"distance\":\"161.834\",\"tel\":[],\"type\":\"科教文化服务;科研机构;科研机构\"},{\"id\":\"B000A209FB\",\"direction\":\"东\",\"businessarea\":\"万泉河\",\"address\":\"颐和园路5号北京大学\",\"poiweight\":\"0.288953\",\"name\":\"北京大学政治发展与政府管理研究所\",\"location\":\"116.311895,39.991290\",\"distance\":\"177.427\",\"tel\":[],\"type\":\"科教文化服务;科研机构;科研机构\"},{\"id\":\"B000A95CZ4\",\"direction\":\"东\",\"businessarea\":\"万泉河\",\"address\":\"颐和园路5号北京大学五四路附近\",\"poiweight\":\"0.242941\",\"name\":\"北京大学妇女研究中心\",\"location\":\"116.311885,39.991245\",\"distance\":\"178.812\",\"tel\":[],\"type\":\"科教文化服务;科研机构;科研机构\"},{\"id\":\"B000A7OJH3\",\"direction\":\"东\",\"businessarea\":\"万泉河\",\"address\":\"颐和园路5号北京大学\",\"poiweight\":\"0.126582\",\"name\":\"北大欧洲中国研究合作中心\",\"location\":\"116.311895,39.991290\",\"distance\":\"177.427\",\"tel\":[],\"type\":\"科教文化服务;科研机构;科研机构\"},{\"id\":\"B000A26DAD\",\"direction\":\"东\",\"businessarea\":\"万泉河\",\"address\":\"颐和园路5号北京大学\",\"poiweight\":\"0.304437\",\"name\":\"北京大学邓小平理论研究中心\",\"location\":\"116.311885,39.991245\",\"distance\":\"178.812\",\"tel\":[],\"type\":\"科教文化服务;科研机构;科研机构\"},{\"id\":\"B000A1B4B7\",\"direction\":\"东\",\"businessarea\":\"万泉河\",\"address\":\"颐和园路5号北京大学凯原楼北京大学\",\"poiweight\":\"0.311729\",\"name\":\"北京大学经济法研究所\",\"location\":\"116.311885,39.991245\",\"distance\":\"178.812\",\"tel\":[],\"type\":\"科教文化服务;科研机构;科研机构\"},{\"id\":\"B000AA11GF\",\"direction\":\"西南\",\"businessarea\":\"万泉河\",\"address\":\"颐和园路5号北京大学燕南园52号\",\"poiweight\":\"0.240818\",\"name\":\"北京大学视觉与图像研究中心\",\"location\":\"116.309175,39.990291\",\"distance\":\"198.219\",\"tel\":[],\"type\":\"科教文化服务;科研机构;科研机构\"},{\"id\":\"B000AA1YH4\",\"direction\":\"东\",\"businessarea\":\"万泉河\",\"address\":\"颐和园路5号北京大学文史教学楼附近\",\"poiweight\":\"0.189818\",\"name\":\"北京大学生物基础教学实验中学生物化学实验室1\",\"location\":\"116.312067,39.992472\",\"distance\":\"184.922\",\"tel\":[],\"type\":\"科教文化服务;科研机构;科研机构\"},{\"id\":\"B0FFG4DZA1\",\"direction\":\"东北\",\"businessarea\":\"万泉河\",\"address\":\"临湖路与未名北路交叉口南50米\",\"poiweight\":\"0.202815\",\"name\":\"北京大学-广播台\",\"location\":\"116.311865,39.992914\",\"distance\":\"191.012\",\"tel\":[],\"type\":\"科教文化服务;传媒机构;传媒机构\"},{\"id\":\"B000A9PIG4\",\"direction\":\"西\",\"businessarea\":\"万泉河\",\"address\":\"颐和园路5号北京大学\",\"poiweight\":\"0.300935\",\"name\":\"北京大学静园六院\",\"location\":\"116.308847,39.992156\",\"distance\":\"100.953\",\"tel\":[],\"type\":\"科教文化服务;学校;学校\"},{\"id\":\"B0FFFDNE72\",\"direction\":\"西\",\"businessarea\":\"万泉河\",\"address\":\"颐和园路5号北京大学\",\"poiweight\":\"0.218941\",\"name\":\"北京大学静园五院\",\"location\":\"116.308908,39.991691\",\"distance\":\"97.8593\",\"tel\":[],\"type\":\"科教文化服务;学校;学校\"},{\"id\":\"B000A80D7Q\",\"direction\":\"东北\",\"businessarea\":\"万泉河\",\"address\":\"颐和园路5号北京大学\",\"poiweight\":\"0.308949\",\"name\":\"北京大学第1教学楼\",\"location\":\"116.310408,39.992835\",\"distance\":\"103.557\",\"tel\":[],\"type\":\"科教文化服务;学校;学校\"},{\"id\":\"B0FFFGYAZZ\",\"direction\":\"西南\",\"businessarea\":\"万泉河\",\"address\":\"颐和园路5号北京大学\",\"poiweight\":\"0.204704\",\"name\":\"北京大学-静园四院\",\"location\":\"116.308928,39.991168\",\"distance\":\"126.82\",\"tel\":[],\"type\":\"科教文化服务;学校;学校\"}]},\"info\":\"OK\",\"infocode\":\"10000\"}"
    val res: String = getJson(json)
    println(res)
  }
}
