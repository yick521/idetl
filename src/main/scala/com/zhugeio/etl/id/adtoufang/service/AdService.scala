package com.zhugeio.etl.id.adtoufang.service


import com.alibaba.fastjson.{JSON, JSONObject}
import com.zhugeio.tool.commons.JsonUtil
import com.zhugeio.etl.id.ZGMessage
import com.zhugeio.etl.id.adtoufang.common.AdMessage
import com.zhugeio.etl.id.adtoufang.common.ToufangMatchFeild.{md5ExcludeSet, originExcludeSet}
import com.zhugeio.etl.id.adtoufang.service.LidAndUserFirstEndService.expireTime
import com.zhugeio.etl.id.adtoufang.util.ToolUtil
import com.zhugeio.etl.id.cache.FrontCache
import com.zhugeio.etl.id.redis.AdRedisClient
import com.zhugeio.etl.id.service.MainService.openToufangLog
import com.zhugeio.etl.id.service.{DistributeService, MainService}
import org.apache.commons.lang.StringUtils
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.log4j.Logger
import java.util
import scala.collection.mutable
import scala.collection.mutable.ListBuffer


object AdService {
  val logger = Logger.getLogger(this.getClass)
  val appPre = "adtfad"
  var utmPre = "utm"

  /**
    * 获取腾讯web广告原数据的key
    */
  def addSet(msgs: ListBuffer[ZGMessage], tengxunWebKeySet: mutable.HashSet[String]): Unit = {
    msgs.foreach(msg => {
      if (msg.rawData.contains("zg_adtoufang") && msg.rawData.contains("$channel_click_id")) {
        val json = JSON.parseObject(msg.rawData)
        if (json != null) {
          val dataArr = json.getJSONArray("data")
          if (dataArr != null && dataArr.size() > 0) {
            for (i <- 0 until dataArr.size()) {
              val json = dataArr.getJSONObject(i)
              if ("adtf".equals(json.getString("dt"))) {
                val prJson = json.getJSONObject("pr")
                val channelClickId = prJson.getString("$channel_click_id")
                tengxunWebKeySet.add(s"${appPre}:${channelClickId}:rawdata")
              }
            }
          }
        }
      }
    })
  }

  /**
    * 从ssdb获取未匹配上腾讯web端广告的原数据
    */
  def addMessage(msgs: ListBuffer[ZGMessage], keySet: mutable.HashSet[String]): Unit = {
    var jedis = AdRedisClient.jedisPool.getResource
    val rawdataHashMap = AdRedisClient.mgetResult(keySet, false)
    val iterator1 = rawdataHashMap.iterator
    val thisDelHashKeySet = mutable.HashSet[String]()
    while (iterator1.hasNext) {
      val tuple = iterator1.next()
      msgs += new ZGMessage("1", 0, 1, "", tuple._2)
      if ("true".equals(openToufangLog)) {
        println("print:AdService 生成adtf 重新匹配腾讯web端广告：" + tuple._1)
      }

      thisDelHashKeySet.add(tuple._1)
    }
    if (thisDelHashKeySet.size > 0) {
      jedis.del(thisDelHashKeySet.toArray: _*)
    }
    if (jedis != null) {
      jedis.close()
    }
  }

  /**
    * 为事件 添加 eid 对应的utm信息
    */
  def addUtm(msgs: ListBuffer[ZGMessage]): Unit = {
    var thisKeySet = mutable.HashSet[String]()
    var thisDataMap = mutable.HashMap[String, String]()
    //$zg_eid
    //获取 key
    getUtmKeys(msgs, thisKeySet)
    //从ssdb 批量获取 value
    if (thisKeySet.size > 0) {
      thisDataMap = AdRedisClient.mgetResult(thisKeySet, false)
      if (thisDataMap.size > 0) {
        //为事件添加utm属性 （内置属性）
        setEventPrUtm(msgs, thisDataMap)
      }
    }
  }

  /**
    * 获取从ssdb取utm数据的key
    */
  private def setEventPrUtm(msgs: ListBuffer[ZGMessage], thisDataMap: mutable.HashMap[String, String]) = {
    msgs.filter(_.result != -1).foreach(
      msg => {
        //这里用的 rawData ，避免上游未处理，msg.data无数据  ?
        // ss -1 ,se -2
        var zgEid = msg.zgeid

        //处理 来自web端+APP端的 广告投放数据，添加用户属性（首次投放链接、后续投放链接）、事件属性（投放链接id lid）
        if (zgEid > 0) {
          val dataItemIterator = msg.data.get("data").asInstanceOf[util.List[util.Map[String, Object]]].iterator()
          while (dataItemIterator.hasNext) {
            val dataItem: util.Map[String, Object] = dataItemIterator.next()
            val dt = dataItem.get("dt").asInstanceOf[String]
            //注意 "mkt"  etl-dw-phoenix工程会处理该类型的消息，未写kudu
            //"evt" == dt || "mkt" == dt || "abp" == dt
            if ("evt" == dt || "abp" == dt) {
              var utmKey = s"${utmPre}:${zgEid}"
              if (thisDataMap.contains(utmKey)) {
                val utmJsonStr = thisDataMap(utmKey)
                if (!StringUtils.isEmpty(utmJsonStr)) {

                  val utmJsonObj = JSON.parseObject(utmJsonStr)
                  // utm_source utm_medium utm_campaign  utm_content utm_term
                  val source = utmJsonObj.getString("utm_source")
                  var medium = utmJsonObj.getString("utm_medium")
                  var campaign = utmJsonObj.getString("utm_campaign")
                  var utmcontent = utmJsonObj.getString("utm_content")
                  var term = utmJsonObj.getString("utm_term")

                  val props = dataItem.get("pr").asInstanceOf[util.Map[String, Object]]
                  props.put("$utm_source", source)
                  props.put("$utm_medium", medium)
                  props.put("$utm_campaign", campaign)
                  props.put("$utm_content", utmcontent)
                  props.put("$utm_term", term)
                }
              }
            }
          }
        }
      }
    )
  }

  /**
    * 获取从ssdb取utm数据的key
    */
  private def getUtmKeys(msgs: ListBuffer[ZGMessage], thisKeySet: mutable.HashSet[String]) = {
    msgs.filter(_.result != -1).foreach(
      msg => {
        //这里用的 rawData ，避免上游未处理，msg.data无数据  ?
        // ss -1 ,se -2
        var zgEid = msg.zgeid
        //处理 来自web端+APP端的 广告投放数据，添加用户属性（首次投放链接、后续投放链接）、事件属性（投放链接id lid）
        if (zgEid > 0) {
          val dataItemIterator = msg.data.get("data").asInstanceOf[util.List[util.Map[String, Object]]].iterator()
          while (dataItemIterator.hasNext) {
            val dataItem: util.Map[String, Object] = dataItemIterator.next()
            val dt = dataItem.get("dt").asInstanceOf[String]
            //注意 "mkt"  etl-dw-phoenix工程会处理该类型的消息，未写kudu
            //"evt" == dt || "mkt" == dt || "abp" == dt
            if ("evt" == dt || "abp" == dt) {
              thisKeySet += s"${utmPre}:${zgEid}"
            }
          }
        }
      }
    )
  }

  /**
    * 存储广告数据
    *
    * @param msgs
    */
  def saveAppAdData(msgs: ListBuffer[ZGMessage]): Unit = {
    var thisAppMaxCtKeySet = mutable.HashSet[String]()
    var lastAppDataKeySet = mutable.HashSet[String]()
    var lastAppSsdbDataMap = mutable.HashMap[String, String]()
    var lastAppSsdbMaxCtMap = mutable.HashMap[String, String]()

    var thisAppMaxCtMap = mutable.HashMap[String, Long]()
    var thisAppDataMap = mutable.HashMap[String, String]()
    var thisAppDelKeySet = mutable.HashSet[String]()

    setIpUaMaps(msgs, thisAppMaxCtKeySet, thisAppMaxCtMap, thisAppDataMap)
    if ("true".equals(openToufangLog)) {
      println("print:AdService app 本批次thisAppMaxCtMap：" + thisAppMaxCtMap)
    }

    //ssdb 获取 （s"${ssdbCtPre}:${appZgidKey}"，ct）
    if (thisAppMaxCtKeySet.size > 0) {
      lastAppSsdbMaxCtMap = AdRedisClient.mgetResult(thisAppMaxCtKeySet, false)
      if ("true".equals(openToufangLog)) {
        println("print:AdService app ssdb获取之前已存入的MaxCt lastAppSsdbMaxCtMap：" + lastAppSsdbMaxCtMap)
      }

      val ctIterator = lastAppSsdbMaxCtMap.iterator
      while (ctIterator.hasNext) {
        val tuple = ctIterator.next()
        val ctKey = tuple._1
        val maxCt = tuple._2
        lastAppDataKeySet += s"${ctKey}:${maxCt}"
      }

      if (lastAppDataKeySet.size > 0) {
        //ssdb 获取 （s"${ssdbCtPre}:${appZgidKey}:${ct}"，s"${lid},${lname}::"）
        lastAppSsdbDataMap = AdRedisClient.mgetResult(lastAppDataKeySet, false)

      }
    }

    //将广告信息存入sssdb
    var jedis = AdRedisClient.jedisPool.getResource
    val pipeline = jedis.pipelined()
    if (thisAppMaxCtMap.size > 0) {
      val iterator = thisAppMaxCtMap.iterator
      while (iterator.hasNext) {
        val tuple = iterator.next()
        val ctKey = tuple._1
        val thisMaxCt = tuple._2
        if (!lastAppSsdbMaxCtMap.contains(ctKey) || thisMaxCt >= lastAppSsdbMaxCtMap(ctKey).toLong) {
          val dataKey = s"${ctKey}:${thisMaxCt}"
          //更新ssdb 最大ct
          if ("true".equals(openToufangLog)) {
            println("print:AdService app 向 ssdb 存入广告信息，ctKey：" + ctKey + "， thisMaxCt：" + thisMaxCt)
          }


          pipeline.set(ctKey, thisMaxCt + "")
          pipeline.expire(ctKey, expireTime) //过期时间设为24小时
          if ("true".equals(openToufangLog)) {
            println("print:AdService app 向 ssdb 存入广告信息，dataKey：" + dataKey + "， thisAppDataMap：" + thisAppDataMap)
          }
          //更新ssdb 最大ct对应的广告信息
          pipeline.set(dataKey, thisAppDataMap.get(dataKey).get)
          pipeline.expire(dataKey, expireTime) //过期时间设为24小时

          if (lastAppSsdbMaxCtMap.contains(ctKey)) {
            val lastMaxCt = lastAppSsdbMaxCtMap.get(ctKey).get.toLong
            if (thisMaxCt > lastMaxCt) {
              thisAppDelKeySet += s"${ctKey}:${lastMaxCt}"
            }
          }

        }
      }
    }
    pipeline.sync()

    //删除过期无效数据
    if (thisAppDelKeySet.size > 0) {
      jedis.del(thisAppDelKeySet.toArray: _*)
    }
    if (jedis != null) {
      jedis.close()
    }

  }

  /**
    * 为map赋值 ip+ua
    */
  private def setIpUaMaps(msgs: ListBuffer[ZGMessage], thisAppMaxCtKeySet: mutable.HashSet[String], thisAppMaxCtMap: mutable.HashMap[String, Long], thisAppDataMap: mutable.HashMap[String, String]) = {
    msgs.filter(_.result != -1).foreach(
      msg => {
        //这里用的 rawData ，避免上游未处理，msg.data无数据  ?
        val map: util.Map[String, AnyRef] = JsonUtil.mapFromJson(msg.rawData)
        val sdk = map.get("sdk").asInstanceOf[String]
        val ak = map.get("ak").toString

        if (!StringUtils.isEmpty(sdk) && sdk.equals("zg_adtoufang")) {
          if (FrontCache.openAdvertisingFunctionAppMap.contains(ak)) {
            val iterator = map.get("data").asInstanceOf[util.List[util.Map[String, Object]]].iterator()
            while (iterator.hasNext) {
              val item = iterator.next()
              if ("adtf".equals(String.valueOf(item.get("dt")))) {
                val props = item.get("pr").asInstanceOf[util.Map[String, Object]]
                val ip: String = String.valueOf(if (props.get("$ip") == null) "" else props.get("$ip"))
                val ua: String = String.valueOf(if (props.get("$ua") == null) "" else props.get("$ua"))
                //app端广告信息中 $ados ： android 0 、 ios 1 、其他 3
                val os: String = String.valueOf(if (props.get("$ados") == null) "" else props.get("$ados"))
                //区分 广告信息来自哪个投放平台：$channel_type：百度信息流 1、巨量引擎 2、腾讯广告 3、百度搜索 4
                val channelType: String = String.valueOf(if (props.get("$channel_type") == null) "" else props.get("$channel_type"))

                val lid: Long = String.valueOf(if (props.get("$lid") == null) 0 else props.get("$lid")).toLong
                //TODO 注意： app端广告信息的lua脚本中的 应用id的key名称叫 $zg_appid, app端没有ct，有 $click_time
                val appId = String.valueOf(props.get("$zg_appid")).toLong
                //app端广告数据：
                if (lid != 0L) {
                  val adms = new AdMessage()
                  adms.zg_appid = appId
                  adms.lid = lid
                  val clickTimeStr = String.valueOf(if (props.get("$click_time") == null) 0 else props.get("$click_time"))
                  var clickTime = clickTimeStr.toLong
                  if (clickTimeStr.length == 10) {
                    clickTime = clickTimeStr.toLong * 1000
                  }
                  adms.click_time = clickTime
                  adms.ct = clickTime
                  adms.app_key = ak
                  adms.setFeilds(props)

                  val uaProcess = getUaProcess(channelType, ua)
                  val ipUaKey = s"${appPre}:${appId}:${ip}${uaProcess}"
                  if (StringUtils.isNotEmpty(ip)) {
                    //TODO 模糊匹配：ip+ua 或 ip+os 或 ip
                    adms.ip_ua_key = ipUaKey
                    thisAppMaxCtKeySet += ipUaKey
                  }

                  //TODO 精确匹配

                  setMuidProcess(channelType, appId + "", adms, thisAppMaxCtKeySet, thisAppMaxCtMap, thisAppDataMap)

                  if (StringUtils.isNotEmpty(ip)) {
                    //TODO 留下模糊匹配：最近一次点击的广告 (adms 需含有精确匹配 对应的key值)
                    val isContainIpUaKey = thisAppMaxCtMap.contains(ipUaKey)
                    var maxCt = 0L
                    if (isContainIpUaKey) {
                      maxCt = thisAppMaxCtMap.get(ipUaKey).get
                    }
                    if (!isContainIpUaKey || adms.ct >= maxCt) {
                      thisAppMaxCtMap.put(ipUaKey, adms.ct)
                      thisAppDataMap.put(s"${ipUaKey}:${adms.ct}", adms.toJsonString())
                    }
                  }
                  //TODO 广告数据发kafka
                  val jsonObj = new JSONObject()
                  jsonObj.put("tableName", "toufang_ad_click")
                  jsonObj.put("sinkType", "kudu")
                  jsonObj.put("data", adms.toJsonString())
                  if (!MainService.test) {
                    DistributeService.simpleSend(new ProducerRecord[String, String](DistributeService.typeTopicMap("toufang_kudu"), s"${appId}:${ipUaKey}", jsonObj.toJSONString.replace("\\\"", "\"").replace("\"{", "{").replace("}\"", "}")))
                  }


                }
              }
            }
          }
        }
      }
    )
  }


  /**
    * app端模糊匹配ua解析 "name:version"
    */
  def getUaProcess(channelType: String, ua: String): String = {
    var uaProcess = ToolUtil.uaAnalysis(ua)
    if ("1".equals(channelType) || "4".equals(channelType)) {
      //$channel_type：百度信息流 1、巨量引擎 2、腾讯广告 3、百度搜索 4
      //百度信息流、百度搜索 的ios 按 ip+版本号匹配 （因ipad 也解析成了iphone）
      if (ua.contains("iPhone")) {
        val arr = uaProcess.split(":")
        if (arr.size > 0) {
          var name = arr(0)
          var version = arr(1)
          uaProcess = ":" + version
        }
      }
    }
    return uaProcess
  }

  /**
    * app端 精确匹配逻辑
    */

  def setMuidProcess(channelType: String, appId: String, adms: AdMessage, thisAppMaxCtKeySet: mutable.HashSet[String], thisAppMaxCtMap: mutable.HashMap[String, Long], thisAppDataMap: mutable.HashMap[String, String]): Unit = {
    //注意事项：因广告点击监测时第三方平台传递数据可能传递“0”、NULL、“”，过滤掉“”、0、NULL和他们的MD5小写值
    var muidProcessKeyHash = mutable.HashMap[String, String]()
    var muid = if (originExcludeSet.contains(adms.muid) || md5ExcludeSet.contains(adms.muid)) "" else (if (adms.muid.length == 32) adms.muid else ToolUtil.getMD5Str(adms.muid))
    var androidId = if (originExcludeSet.contains(adms.android_id) || md5ExcludeSet.contains(adms.android_id)) "" else (if (adms.android_id.length == 32) adms.android_id else ToolUtil.getMD5Str(adms.android_id))
    var oaid = if (originExcludeSet.contains(adms.oaid) || md5ExcludeSet.contains(adms.oaid)) "" else (if (adms.oaid.length == 32) adms.oaid else ToolUtil.getMD5Str(adms.oaid))
    var idfa = if (originExcludeSet.contains(adms.idfa) || md5ExcludeSet.contains(adms.idfa)) "" else (if (adms.idfa.length == 32) adms.idfa else ToolUtil.getMD5Str(adms.idfa))
    var imei = if (originExcludeSet.contains(adms.imei) || md5ExcludeSet.contains(adms.imei)) "" else (if (adms.imei.length == 32) adms.imei else ToolUtil.getMD5Str(adms.imei))
    //app端广告信息中 $ados ： android 0 、 ios 1 、其他 3
    //区分 广告信息来自哪个投放平台：$channel_type：百度信息流 1、巨量引擎 2、腾讯广告 3、百度搜索 4 、知乎 9、快手 10、微博 11、华为 12
    // muid_key idfa_key  imei_key  android_id_key oaid_key
    //对md5值转小写
    muid=muid.toLowerCase
    androidId=androidId.toLowerCase
    oaid=oaid.toLowerCase
    idfa = idfa.toLowerCase
    imei=imei.toLowerCase
//    if("10".equals(channelType) && "10".equals(adms.push_type)){
//      //快手ios的idfa加密是大写-存广告时需要转成小写
//      idfa=idfa.toLowerCase
//    }

    if ("3".equals(channelType) && "20".equals(adms.push_type) && StringUtils.isNotEmpty(adms.channel_click_id)) {
      //push_type：10 app、20 web广告数据  腾讯广告 -web:channel_click_id
      if (StringUtils.isNotEmpty(adms.channel_click_id)) {
        val muidProcessKey = s"${appPre}:${appId}:${adms.channel_click_id}"
        adms.channel_click_id_key = muidProcessKey
        thisAppMaxCtKeySet += muidProcessKey
        putAd(muidProcessKey, adms, thisAppMaxCtMap, thisAppDataMap)
      }
    } else {

      if (StringUtils.isNotEmpty(muid)) {
        val muidProcessKey = s"${appPre}:${appId}:${muid}"
        adms.muid_key = muidProcessKey
        thisAppMaxCtKeySet += muidProcessKey
        putAd(muidProcessKey, adms, thisAppMaxCtMap, thisAppDataMap)
      }

      if (StringUtils.isNotEmpty(idfa)) {
        //ios
        val muidProcessKey = s"${appPre}:${appId}:${idfa}"
        adms.idfa_key = muidProcessKey
        thisAppMaxCtKeySet += muidProcessKey
        putAd(muidProcessKey, adms, thisAppMaxCtMap, thisAppDataMap)
      }
      if (StringUtils.isNotEmpty(imei)) {
        val muidProcessKey = s"${appPre}:${appId}:${imei}"
        adms.imei_key = muidProcessKey
        thisAppMaxCtKeySet += muidProcessKey
        putAd(muidProcessKey, adms, thisAppMaxCtMap, thisAppDataMap)
      }
      if (StringUtils.isNotEmpty(androidId)) {
        val muidProcessKey = s"${appPre}:${appId}:${androidId}"
        adms.android_id_key = muidProcessKey
        thisAppMaxCtKeySet += muidProcessKey
        putAd(muidProcessKey, adms, thisAppMaxCtMap, thisAppDataMap)
      }
      if (StringUtils.isNotEmpty(oaid)) {
        val muidProcessKey = s"${appPre}:${appId}:${oaid}"
        adms.oaid_key = muidProcessKey
        thisAppMaxCtKeySet += muidProcessKey
        putAd(muidProcessKey, adms, thisAppMaxCtMap, thisAppDataMap)
      }

    }

  }

  /**
    * 留下精确匹配：最近一次点击的广告
    */
  def putAd(muidProcessKey: String, adms: AdMessage, thisAppMaxCtMap: mutable.HashMap[String, Long], thisAppDataMap: mutable.HashMap[String, String]): Unit = {
    //TODO 留下精确匹配：最近一次点击的广告
    val isContainMuidKey = thisAppMaxCtMap.contains(muidProcessKey)
    var muidMaxCt = 0L
    if (isContainMuidKey) {
      muidMaxCt = thisAppMaxCtMap.get(muidProcessKey).get
    }
    if (!isContainMuidKey || adms.ct >= muidMaxCt) {
      thisAppMaxCtMap.put(muidProcessKey, adms.ct)
      thisAppDataMap.put(s"${muidProcessKey}:${adms.ct}", adms.toJsonString())
    }
  }


}
