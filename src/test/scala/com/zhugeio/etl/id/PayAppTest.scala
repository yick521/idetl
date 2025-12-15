package com.zhugeio.etl.id


import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}
import com.zhugeio.etl.adtoufang.common.OperatorUtil
import com.zhugeio.tool.commons.JsonUtil
import com.zhugeio.etl.id.adtoufang.common.ToufangMatchFeild
import com.zhugeio.etl.id.adtoufang.service.AdService
import com.zhugeio.etl.id.adtoufang.service.LidAndUserFirstEndService.appPre
import com.zhugeio.etl.id.adtoufang.util.ToolUtil
import com.zhugeio.etl.id.log.IdLogger
import com.zhugeio.etl.id.redis.AdRedisClient
import com.zhugeio.etl.id.service.MainService
import com.zhugeio.etl.id.service.VirtualEventService.getCompareResult
import org.junit.Test

import java.io.{BufferedReader, InputStreamReader}
import scala.collection.mutable.ListBuffer
import useragentanalysis.UserAgent

import java.{lang, util}
import scala.collection.mutable

/**
  * Created by ziwudeng on 10/17/16.
  */
class PayAppTest {
  @Test
  def testMain(): Unit = {
    val ir = new BufferedReader(new InputStreamReader(this.getClass.getClassLoader.getResourceAsStream("sample_web.txt")))
    val iterator = ir.lines().iterator();
    while (iterator.hasNext) {
      val line = iterator.next();
      val msg = new ZGMessage("1", 1, 1, "1", line)
      val a = new ListBuffer[ZGMessage]()
      a += msg;
      MainService.process(a);
      a.foreach(msg => {
        //IdLogger.info(" msg.result=>" + msg.result)
        println("msg.result=>" + msg.result)
        println("msg.error=>" + msg.error)
        println("rawData=>" + msg.rawData)
        println("result=>" + JsonUtil.toJson(msg.data))
      })
    }
  }


  @Test
  def testPg(): Unit = {
    // var value = FrontAdPgDao.getJdbcTemplate.queryForList("select * from toufang_convert_event limit 10");
    // println(value)
    //   val ur = UserAgent.parseUserAgentString("Mozilla/5.0 (iPhone; CPU iPhone OS 16 0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Mobile/15E148 MicroMessenger/8.0.28(0x18001c29) NetType/WIFI Language/zh CN")
    //    val ur1 = UserAgent.parseUserAgentString("Mozilla/5.0 (iPhone: CPU iPhone OS 16 0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Mobile/20A362 AliApp(DingTalk/6.5.41) com.laiwang.DingTalk/26030762 Channel/201200")
//    val ur = UserAgent.parseUserAgentString("Mozilla/5.0 (Linux; Android 12; LGE-AN00 Build/BRAND-MODEL; wv) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/76.0.3809.89 Mobile Safari/537.36 T7/12.9 SP-engine/2.21.0 matrixstyle/0 lite baiduboxapp/5.4.0.10 (Baidu; P1 8.1.0) NABar/1.0")
//    val ur1 = UserAgent.parseUserAgentString("Dalvik/2.1.0 (Linux; U; Android 12; LGE-AN00 Build/HONORLGE-AN00)")
    val ur = UserAgent.parseUserAgentString("Mozilla/5.0 (iPhone; CPU iPhone OS 14_0_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Mobile/15E148")
    val ur1 = UserAgent.parseUserAgentString("Mozilla/5.0 (iPhone; CPU iPhone OS 15.6.1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Mobile/15E148 swan/2.26.0 swan-baiduboxapp/12.6.5.10 baiduboxapp/12.6.5.10 (Baidu; P2 14.4.2)")
    val uaProcess1 = new StringBuilder().append(ur.getId).append(ur.getBrowser).append(ur.getOperatingSystem)

    println(ur.getId)
    println(ur.getBrowser)
    println(ur.getOperatingSystem)
    println(ur.getBrowserVersion)
    println(uaProcess1)

    val uaProcess2 = new StringBuilder().append(ur1.getId).append(ur1.getBrowser).append(ur1.getOperatingSystem)
    println(ur1.getId)
    println(ur1.getBrowser)
    println(ur1.getOperatingSystem)

    println(uaProcess2)
  }
  @Test
  def testStr(): Unit = {
    var str="1663153525937"
    var os="iOS"
    println(os.toLowerCase().equals("ios"))
    println(str.toLong)

  }

  /**
    * 虚拟事件属性过滤调试
    */
  @Test
  def testVirtualEvent(): Unit = {

//    var ua="Dalvik/2.1.0 (Linux; U; Android 12; LGE-AN00 Build/HONORLGE-AN00)"
//    println(ToolUtil.uaAnalysis(ua))
    var str="{\"sln\":\"itn\",\"pl\":\"js\",\"sdk\":\"zg-js\",\"sdkv\":\"2.0\",\"owner\":\"zg\",\"ut\":\"2023-11-27 18:58:56\",\"tz\":28800000,\"debug\":1,\"ak\":\"6b7f3c2bd6b143279f96c4deab504299\",\"usr\":{\"did\":\"c3883ea3-6407-4892-b694-8708b7da94ac\"},\"data\":[{\"dt\":\"abp\",\"pr\":{\"$ct\":1701082736534,\"$tz\":28800000,\"$eid\":\"revenue\",\"$cn\":\"js\",\"$sid\":20231127185855014100,\"$cuid\":\"1\",\"$url\":\"https://www.baidu.com\",\"$ref\":\"www.baidu.com\",\"$referrer_domain\":\"www.baidu.com\",\"$utm_source\":\"baidu\",\"$utm_medium\":\"公众号搜索\",\"$utm_campaign\":\"双11促销\",\"$utm_content\":\"���放广告\",\"$utm_term\":\"付费\",\"$price\":1000,\"$total\":2,\"$productID\":\"华为Mate30 Pro\",\"$productQuantity\":1,\"$revenueType\":\"移动电话\"}}],\"ip\":\"47.92.192.89\",\"st\":1701082798226,\"ua\":\"Java/11.0.15.1\"}"
    var eventMatchJsonStr="{\"owner\":\"zg_abp\",\"eventId\":18897,\"count\":4,\"alias\":\"收入\",\"eventName\":\"revenue\",\"filters\":{\"conditions\":[{\"attrId\":98572,\"propCategory\":\"eventProp\",\"values\":[\"移动电话\"],\"dimensionSub\":\"event_attr\",\"label\":\"revenueType\",\"type\":1,\"operator\":\"equal\",\"attrName\":\"revenueType\"},{\"attrId\":98569,\"propCategory\":\"eventProp\",\"values\":[],\"dimensionSub\":\"event_attr\",\"label\":\"productID\",\"type\":1,\"operator\":\"is not null\",\"attrName\":\"productID\"},{\"attrId\":98578,\"propCategory\":\"eventProp\",\"values\":[\"https://www.baidu.com\"],\"dimensionSub\":\"event_attr\",\"label\":\"url\",\"type\":1,\"operator\":\"equal\",\"attrName\":\"url\"},{\"attrId\":98585,\"propCategory\":\"eventProp\",\"values\":[],\"dimensionSub\":\"event_attr\",\"label\":\"ref\",\"type\":1,\"operator\":\"is not null\",\"attrName\":\"ref\"}],\"relation\":\"and\"}}"
    val map: java.util.Map[String, AnyRef] = JsonUtil.mapFromJson(str)
    val dataItemIterator = map.get("data").asInstanceOf[java.util.List[java.util.Map[String, java.lang.Object]]].iterator()
    while (dataItemIterator.hasNext) {
      val dataItem = dataItemIterator.next()
      val dt = String.valueOf(dataItem.get("dt"))
      val pr = dataItem.get("pr").asInstanceOf[java.util.Map[String, java.lang.Object]]
      val eventJsonObj: JSONObject = JSON.parseObject(eventMatchJsonStr)
      val filters = eventJsonObj.getJSONObject("filters")
      var compareResult = false
      if(filters!=null){
        val matchJsonArr = filters.getJSONArray("conditions")
        if(matchJsonArr!=null){
          compareResult = getCompareResult(dt, pr, matchJsonArr)
          println("compareResult1:"+compareResult)
        }else{
          compareResult=true
          println("compareResult2:"+compareResult)
        }
      }else{
        compareResult=true
        println("compareResult3:"+compareResult)
      }
      println("compareResult:"+compareResult)

    }

  }
  def getCompareResult(dt:String,pr:java.util.Map[String, java.lang.Object],matchJsonArr: JSONArray): Boolean = {
    matchJsonArr.forEach(
      matchJsonObj => {
        val matchJson = matchJsonObj.asInstanceOf[JSONObject]
        val label = matchJson.getString("label") // 如 "_广告分析链接ID":183,
        var eventAttValue = String.valueOf(pr.get("_" + label))
        println("_label:"+label)
        if ("abp" == dt) {
          //获取内置事件属性值
          eventAttValue = String.valueOf(pr.get("$" + label))
          println("$label:"+label)
        }

        println("eventAttValue:"+eventAttValue)
        val boolean: lang.Boolean = OperatorUtil.compareValue(eventAttValue, matchJson)
        if (!boolean) {
          return false
        }
      })
    return true
  }
}
