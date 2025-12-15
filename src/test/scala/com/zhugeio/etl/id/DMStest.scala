package com.zhugeio.etl.id

import com.alibaba.fastjson.{JSON, JSONObject}
import com.zhugeio.etl.commons.Dims
import com.zhugeio.tool.commons.JsonUtil
import org.junit.Test

import java.time.format.DateTimeFormatter
import java.time.{LocalDate, LocalDateTime, ZoneOffset}
import java.util
import scala.collection.mutable


/**
  * Created by Administrator on 2017/3/28.
  */
class DMStest {

  @Test
  def testMap(): Unit = {
    val map = new mutable.HashMap[String, String]()
    map.put("aa", "cc")
    map.put("bb", "dd")
    var i = 0
    map.foreach(m => {
      i = i + 1
      println(i)
    })

  }

  @Test
  def testPlat(): Unit = {
    val plat: Integer = Dims.sdk("WXA")
    println(plat)
  }

  @Test
  def testDate(): Unit = {

    val DATE_STR = "yyyy-MM-dd"
    val DATE_FORMATTER = DateTimeFormatter.ofPattern(DATE_STR)
    val date = LocalDate.parse("2022-09-12", DATE_FORMATTER)
    var timestamp = date.atStartOfDay(ZoneOffset.ofHours(8)).toInstant.toEpochMilli
    println(timestamp)
  }

  @Test
  def test1(): Unit = {

    //var adStr="{ \n\"Version3.1\" = { \n\"iad-attribution\" = true; \n\"iad-org-name\" = \"org name\";\n\"iad-org-id\" = \"40669820\";\n\"iad-campaign-id\" = \"542370539\"; \n\"iad-campaign-name\" = \"campaign name\"; \n\"iad-purchase-date\" = \"2020-08-04T17:18:07Z\" \n\"iad-conversion-date\" = \"2020-08-04T17:18:07Z\"; \n\"iad-conversion-type\" = \"newdownload\"; \n\"iad-click-date\" = \"2020-08-04T17:17:00Z\"; \n\"iad-adgroup-id\" = \"542317095\"; \n\"iad-adgroup-name\" = \"adgroup name\"; \n\"iad-country-or-region\" = \"US\"; \n\"iad-keyword\" = \"keyword\";\n\"iad-keyword-id\" = \"87675432\";\n\"iad-keyword-matchtype\" = \"Broad\";\n\"iad-ad-id\" = \"542317136\";\n}"
    var adStr1 = "{\"usr\":{\"did\":\"2F017B8D-EB4D-4DE2-BB82-78234D191027\"},\"tz\":28800000,\"owner\":\"zg\",\"debug\":0,\"ak\":\"6356d38afa0e42c3abf83925bfac3db3\",\"sln\":\"itn\",\"pl\":\"ios\",\"ut\":\"2023-01-03 14:56:59\",\"data\":[{\"dt\":\"adtf\",\"pr\":{\"$apple_ad\":\"{ \\n\\\"Version3.1\\\" = { \\n\\\"iad-attribution\\\" = true; \\n\\\"iad-org-name\\\" = \\\"org name\\\";\\n\\\"iad-org-id\\\" = \\\"40669820\\\";\\n\\\"iad-campaign-id\\\" = \\\"542370539\\\"; \\n\\\"iad-campaign-name\\\" = \\\"campaign name\\\"; \\n\\\"iad-purchase-date\\\" = \\\"2020-08-04T17:18:07Z\\\" \\n\\\"iad-conversion-date\\\" = \\\"2020-08-04T17:18:07Z\\\"; \\n\\\"iad-conversion-type\\\" = \\\"newdownload\\\"; \\n\\\"iad-click-date\\\" = \\\"2020-08-04T17:17:00Z\\\"; \\n\\\"iad-adgroup-id\\\" = \\\"542317095\\\"; \\n\\\"iad-adgroup-name\\\" = \\\"adgroup name\\\"; \\n\\\"iad-country-or-region\\\" = \\\"US\\\"; \\n\\\"iad-keyword\\\" = \\\"keyword\\\";\\n\\\"iad-keyword-id\\\" = \\\"87675432\\\";\\n\\\"iad-keyword-matchtype\\\" = \\\"Broad\\\";\\n\\\"iad-ad-id\\\" = \\\"542317136\\\";\\n}\",\"$idfa\":\"\",\"$sl\":\"zh\",\"$channel_type\":5,\"$os\":\"iOS\",\"$cr\":\"46007\",\"$tz\":28800000,\"$ct\":1672729019135}}],\"sdkv\":\"3.4.27\",\"sdk\":\"zg-ios\",\"ip\":\"119.97.197.5\",\"st\":1672729019248,\"ua\":\"Mozilla/5.0 (iPhone; CPU iPhone OS 15_3_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Mobile/15E148\"}"
    val map: java.util.Map[String, AnyRef] = JsonUtil.mapFromJson(adStr1)
    val dataItemIterator = map.get("data").asInstanceOf[util.List[util.Map[String, Object]]].iterator()
    while (dataItemIterator.hasNext) {
      val dataItem: util.Map[String, Object] = dataItemIterator.next()
      val props = dataItem.get("pr").asInstanceOf[util.Map[String, Object]]
      var adStr = props.get("$apple_ad").asInstanceOf[String]

      val json = new JSONObject()
      val arr = adStr.split(";")
      for (value <- arr) {
        val str = value.split("=")
        if (str(0).contains("iad-org-id")) {
          json.put("orgId", str(1).replace("\"", "").trim.toLong)
        }
        if (str(0).contains("iad-campaign-id")) {
          json.put("campaignId", str(1).replace("\"", "").trim.toLong)
        }
        if (str(0).contains("iad-adgroup-id")) {
          json.put("adGroupId", str(1).replace("\"", "").trim.toLong)
        }
        if (str(0).contains("iad-keyword-id")) {
          json.put("keywordId", str(1).replace("\"", "").trim.toLong)
        }
        if (str(0).contains("iad-ad-id")) {
          json.put("adId", str(1).replace("\"", "").trim.toLong)
        }
        if (str(0).contains("iad-click-date")) {
          json.put("clickDate", str(1).replace("\"", "").trim)
        }
      }
      println(json)
    }
    //10.0以上到14.3 的苹果广告数据需进行转换

  }
}
