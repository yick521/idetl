package com.zhugeio.etl.id.adtoufang.service


import com.alibaba.fastjson.{JSON, JSONObject}
import com.zhugeio.tool.commons.JsonUtil
import com.zhugeio.etl.id.ZGMessage
import com.zhugeio.etl.id.adtoufang.common.AdMessage
import com.zhugeio.etl.id.adtoufang.common.ToufangMatchFeild.{md5ExcludeSet, originExcludeSet}
import com.zhugeio.etl.id.adtoufang.util.ToolUtil
import com.zhugeio.etl.id.cache.FrontCache
import com.zhugeio.etl.id.redis.{AdRedisClient, RedisClient}
import com.zhugeio.etl.id.service.MainService.openToufangLog
import com.zhugeio.etl.id.service.{DistributeService, MainService}
import org.apache.commons.lang.StringUtils
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.log4j.Logger


import java.util
import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
  * Created by yanghongyue on 2022/9/1.
  */
object LidAndUserFirstEndService {

  val logger = Logger.getLogger(this.getClass)
  var userFirstLidName = "_首次广告来源"
  var userFollowLidName = "_末次广告来源"
  var lidName = "_广告分析链接ID"

  var channelTypeName = "_广告渠道ID"
  var channelTypeNameStr = "_广告渠道名称"
  var channelAccountIdName = "_广告账号ID"
  var channelCampaignIdName = "_广告计划ID"
  var channelAdgroupIdName = "_广告组ID"
  var channelAdIdName = "_广告创意ID"
  var channelKeywordIdName = "_广告关键词ID"

  // ct 和 lid lname共用前缀
  var ssdbCtPre = "adtfuser" // adtf:ct
  var ssdbAdDataPre = "adtfdata"
  val appPre = "adtfad"
  var expireTime = AdRedisClient.getProValue("lid.expireTime").toInt //24h
  var tengxunWebExpireTime = AdRedisClient.getProValue("tengxun.web.expireTime").toInt //60s
  var convertExpireTime = AdRedisClient.getProValue("lid.convert.expireTime").toLong //30天
  var unmatch = "adtfad:unmatch"

  def handleLidAndUserFirstFollow(msgs: ListBuffer[ZGMessage]): Unit = {
    val thisDelKeySet = mutable.HashSet[String]()
    val thisMaxCtKeySet = mutable.HashSet[String]()
    val thisMaxCtMap = mutable.HashMap[String, Long]()
    val thisDataMap = mutable.HashMap[String, String]()

    val thisLIdLnameMap = mutable.HashMap[String, String]()

    var lastSsdbMaxCtMap = mutable.HashMap[String, String]()
    val lastLIdLnameKeySet = mutable.HashSet[String]()
    var lastLIdLnameMap = mutable.HashMap[String, String]()

    var updateSsdbMaxCtMap = mutable.HashMap[String, String]()
    val updateLIdLnameKeySet = mutable.HashSet[String]()
    var updateLIdLnameMap = mutable.HashMap[String, String]()

    val updateForUserPrKeySet = mutable.HashSet[String]()

    val thisAppMaxCtKeySet = mutable.HashSet[String]()
    var thisAppSsdbMaxCtMap = mutable.HashMap[String, String]()

    val thisAppDataKeySet = mutable.HashSet[String]()
    var thisAppSsdbDataMap = mutable.HashMap[String, String]()

    //TODO 取 app端 ip+ua 对应的广告数据


    setAppIpUaKeys(msgs, thisAppMaxCtKeySet)
    if ("true".equals(openToufangLog)) {
      println("print:LidAndUserFirstEndService app 本批次ip +ua thisAppMaxCtKeySet：" + thisAppMaxCtKeySet)
    }

    //ssdb 获取 （s"${ssdbCtPre}:${appZgidKey}"，ct）
    if (thisAppMaxCtKeySet.nonEmpty) {
      thisAppSsdbMaxCtMap = AdRedisClient.mgetResult(thisAppMaxCtKeySet, false)
      if ("true".equals(openToufangLog)) {
        println("print:LidAndUserFirstEndService app ssdb获取之前已存入的MaxCt thisAppSsdbMaxCtMap：" + thisAppSsdbMaxCtMap)
      }

      val ctIterator = thisAppSsdbMaxCtMap.iterator
      while (ctIterator.hasNext) {
        val tuple = ctIterator.next()
        val ctKey = tuple._1
        val maxCt = tuple._2
        thisAppDataKeySet += s"${ctKey}:${maxCt}"
      }
      if (thisAppDataKeySet.nonEmpty) {
        //ssdb 获取 （s"${ssdbCtPre}:${appZgidKey}:${ct}"，s"${lid},${lname}::"）
        thisAppSsdbDataMap = AdRedisClient.mgetResult(thisAppDataKeySet, false)
        if ("true".equals(openToufangLog)) {
          println("print:LidAndUserFirstEndService app ssdb获取之前已存入的data thisAppSsdbDataMap：" + thisAppSsdbDataMap)
        }
      }
    }



    //TODO 获取取 zgid对应的广告数据
    setAdMaps(msgs, thisMaxCtKeySet, thisMaxCtMap, thisDataMap, thisLIdLnameMap, thisAppSsdbMaxCtMap, thisAppSsdbDataMap)

    if (thisMaxCtKeySet.size > 0) {
      //ssdb 获取 （s"${ssdbCtPre}:${appZgidKey}"，ct）
      lastSsdbMaxCtMap = AdRedisClient.mgetResult(thisMaxCtKeySet, false)
      if ("true".equals(openToufangLog)) {
        println("print:LidAndUserFirstEndService web+app ssdb获取之前已存入的MaxCt lastSsdbMaxCtMap：" + lastSsdbMaxCtMap)
      }

      val ctIterator = lastSsdbMaxCtMap.iterator
      while (ctIterator.hasNext) {
        val tuple = ctIterator.next()
        val ctKey = tuple._1
        val maxCt = tuple._2
        lastLIdLnameKeySet += s"${ctKey}:${maxCt}"
      }
      if (lastLIdLnameKeySet.size > 0) {
        //ssdb 获取 （s"${ssdbCtPre}:${appZgidKey}:${ct}"，s"${lid},${lname}::"）
        lastLIdLnameMap = AdRedisClient.mgetResult(lastLIdLnameKeySet, false)
        if ("true".equals(openToufangLog)) {
          println("print:LidAndUserFirstEndService web+app ssdb获取之前已存入的LIdLname lastLIdLnameMap：" + lastLIdLnameMap)
        }

      }
    }


    //更新ssdb 最大ct
    //TODO 更新ssdb中 用户最近的ct
    if (thisMaxCtMap.size > 0) {
      var jedis = AdRedisClient.jedisPool.getResource
      val pipeline = jedis.pipelined()
      if ("true".equals(openToufangLog)) {
        println("print:LidAndUserFirstEndService web+app thisMaxCtMap:" + thisMaxCtMap)
      }

      val thisCtIter = thisMaxCtMap.iterator
      while (thisCtIter.hasNext) {
        val tuple = thisCtIter.next()
        // s"${appId}:${zgid}"
        val appZgidKey = tuple._1
        val thisMaxCt = tuple._2

        val ctKey = s"${ssdbCtPre}:${appZgidKey}"

        if (!lastSsdbMaxCtMap.contains(ctKey) || (thisMaxCt >= lastSsdbMaxCtMap.get(ctKey).get.toLong)) {
          updateForUserPrKeySet.add(appZgidKey)
          val dataKey = s"${ssdbAdDataPre}:${appZgidKey}:${thisMaxCt}"
          val lidLnameKey = s"${ssdbCtPre}:${appZgidKey}:${thisMaxCt}"

          var lastMaxCt = 0L
          var lastLidLnames = ""
          if (lastSsdbMaxCtMap.contains(ctKey)) {
            lastMaxCt = lastSsdbMaxCtMap.get(ctKey).get.toLong
            if (lastLIdLnameMap.contains(s"${ctKey}:${lastMaxCt}")) {
              lastLidLnames = lastLIdLnameMap.get(s"${ctKey}:${lastMaxCt}").get
            }
          }

          //更新ssdb 最大ct
          pipeline.set(ctKey, thisMaxCt + "")

          //更新ssdb 最大ct对应的广告信息
          pipeline.set(dataKey, thisDataMap.get(s"${appZgidKey}:${thisMaxCt}").get)

          //更新ssdb 最大ct对应的lid lname
          var thisLidLname = thisLIdLnameMap.get(s"${appZgidKey}:${thisMaxCt}").get
          if ("true".equals(openToufangLog)) {
            println("print:LidAndUserFirstEndService web+app thisLIdLnameMap 获取 " + thisLidLname)
          }

          if (!StringUtils.isEmpty(lastLidLnames)) {
            val lastLidLname = lastLidLnames.split("::")(0)
            if (!lastLidLname.equals(thisLidLname)) {
              thisLidLname = s"$lastLidLname::${thisLidLname}"
            }
          }
          pipeline.set(lidLnameKey, thisLidLname)
          if ("true".equals(openToufangLog)) {
            println("print:LidAndUserFirstEndService web+app 存入ssdb lidLnameKey:" + lidLnameKey + " ,lidLnames:" + thisLidLname)
          }

          val dataJson = new JSONObject()
          dataJson.put("key_ad_time", ctKey)
          dataJson.put("key_ad_data", dataKey)
          dataJson.put("key_ad_lid", lidLnameKey)
          dataJson.put("value_ad_time", thisMaxCt + "")
          dataJson.put("value_ad_data", thisDataMap.get(s"${appZgidKey}:${thisMaxCt}").get)
          dataJson.put("value_ad_lid", thisLidLname)
          val jsonObj = new JSONObject()
          jsonObj.put("tableName", "toufang_ad_click")
          jsonObj.put("sinkType", "kudu")
          jsonObj.put("data", dataJson.toJSONString.replace("\\\"", "\"").replace("\"{", "{").replace("}\"", "}"))
          if (!MainService.test) {
            DistributeService.simpleSend(new ProducerRecord[String, String](DistributeService.typeTopicMap("toufang_kudu"), s"${ctKey}", jsonObj.toJSONString.replace("\\\"", "\"").replace("\"{", "{").replace("}\"", "}")))
          }
          //删除过期无效的数据

          if (thisMaxCt > lastMaxCt) {
            thisDelKeySet += s"${ssdbAdDataPre}:${appZgidKey}:${lastMaxCt}"
            thisDelKeySet += s"${ssdbCtPre}:${appZgidKey}:${lastMaxCt}"
          }
        }
      }
      pipeline.sync()
      //删除过期无效数据
      if (thisDelKeySet.size > 0) {
        jedis.del(thisDelKeySet.toArray: _*)
      }
      if (jedis != null) {
        jedis.close()
      }
    }

    if ("true".equals(openToufangLog)) {
      println("print:LidAndUserFirstEndService web+app 用户属性变更 updateForUserPrKeySet：" + updateForUserPrKeySet)
    }

    if (thisMaxCtKeySet.size > 0) {
      //ssdb 获取 更新后的（s"${ssdbCtPre}:${appZgidKey}"，ct）
      updateSsdbMaxCtMap = AdRedisClient.mgetResult(thisMaxCtKeySet, false)
      if ("true".equals(openToufangLog)) {
        println("print:LidAndUserFirstEndService web+app ssdb更新后的MaxCt updateSsdbMaxCtMap：" + updateSsdbMaxCtMap)
      }

      val ctUpdateIterator = updateSsdbMaxCtMap.iterator
      while (ctUpdateIterator.hasNext) {
        val tuple = ctUpdateIterator.next()
        val ctKey = tuple._1
        val maxCt = tuple._2
        updateLIdLnameKeySet += s"${ctKey}:${maxCt}"
      }
      if (updateLIdLnameKeySet.size > 0) {
        //ssdb 获取 更新后的（s"${ssdbCtPre}:${appZgidKey}:${ct}"，s"${lid},${lname}::"）
        updateLIdLnameMap = AdRedisClient.mgetResult(updateLIdLnameKeySet, false)
        if ("true".equals(openToufangLog)) {
          println("print:LidAndUserFirstEndService web+app ssdb更新后的 updateLIdLnameMap：" + updateLIdLnameMap)
        }

      }
    }

    //TODO 带lid的数据 才添加用户属性 （首次投放链接 、后续投放链接）
    // 事件属性 data.pr：  不带lid的需要加上lid
    addUserProAndEventPro(msgs, updateSsdbMaxCtMap, updateLIdLnameMap, updateForUserPrKeySet)
  }

  /**
    * 增加用户属性（首次、后续）、事件属性（lid）
    */
  private def addUserProAndEventPro(msgs: ListBuffer[ZGMessage], updateSsdbMaxCtMap: mutable.HashMap[String, String], updateLIdLnameMap: mutable.HashMap[String, String], updateForUserPrKeySet: mutable.HashSet[String]) = {
    msgs.foreach(msg => {
      try {
        if (msg.result != -1) {

          val ak = msg.data.get("ak").asInstanceOf[String]
          val pl = msg.data.get("pl").asInstanceOf[String]
          val appId = msg.appId



          //处理 来自web端+APP端的 广告投放数据，添加用户属性（首次投放链接、后续投放链接）、事件属性（投放链接id lid）
          if (FrontCache.openAdvertisingFunctionAppMap.contains(ak)) {
            var tz = msg.data.get("tz")
            val dataList = msg.data.get("data").asInstanceOf[util.List[util.Map[String, Object]]]
            val dataItemIterator = dataList.iterator()

            val lidLnameJsonObj = new JSONObject()
            lidLnameJsonObj.put("firstLid", -1)
            lidLnameJsonObj.put("firstLname", "")
            lidLnameJsonObj.put("folLid", -1)
            lidLnameJsonObj.put("folLname", "")


            var hasAddUsrPr = false
            //$zg_did $zg_zgid $ct
            var thisZgdid: Long = -1L
            var zgid: Long = -1L
            var thisCt: Long = -1L
            var lastLid: Integer = -1
            //var lid: Integer = -1
            var admsJson = new JSONObject()
            while (dataItemIterator.hasNext) {

              val dataItem: util.Map[String, Object] = dataItemIterator.next()

              //TODO 添加事件属性 lid
              if ("evt".equals(dataItem.get("dt").asInstanceOf[String])) {
                val props = dataItem.get("pr").asInstanceOf[util.Map[String, Object]]

                thisZgdid = String.valueOf(if (props.get("$zg_did") == null) -1 else props.get("$zg_did")).toLong
                thisCt = String.valueOf(if (props.get("$ct") == null) -1 else props.get("$ct")).toLong


                zgid = props.get("$zg_zgid").asInstanceOf[Long]

                //获取用户首次 后续 lid lname
                if (StringUtils.isEmpty(lidLnameJsonObj.getString("firstLname"))) {
                  val tuple = getUserLastLidLname(appId, zgid, lidLnameJsonObj, updateSsdbMaxCtMap, updateLIdLnameMap, thisCt)
                  lastLid = tuple._1
                  admsJson = tuple._2
                }
                if ("true".equals(openToufangLog)) {
                  println("print:LidAndUserFirstEndService evt 添加事件属性：" + lastLid)
                }

                //添加用户属性、事件属性 lid
                if (lastLid != -1) {
                  //使用匹配到的最近的lid
                  props.put(lidName, lastLid)
                  val channelType = admsJson.getString("channel_type")
                  val channelAccountId = admsJson.getString("channel_account_id")
                  val channelCampaignId = admsJson.getLongValue("channel_campaign_id")
                  val channelAdgroupIdN = admsJson.getLongValue("channel_adgroup_id")
                  val channelAdId = admsJson.getLongValue("channel_ad_id")
                  val channelKeywordId = admsJson.getLongValue("channel_keyword_id")

                  if (!StringUtils.isEmpty(channelType)) props.put(channelTypeName, channelType)
                  if (!StringUtils.isEmpty(channelType)) props.put(channelTypeNameStr, getchannelTypeNameStr(channelType))
                  if (!StringUtils.isEmpty(channelAccountId)) props.put(channelAccountIdName, channelType + "$" + channelAccountId + "")
                  if (channelCampaignId != 0L) props.put(channelCampaignIdName, channelType + "$" + channelCampaignId + "")
                  if (channelAdgroupIdN != 0L) props.put(channelAdgroupIdName, channelType + "$" + channelAdgroupIdN + "")
                  if (channelAdId != 0L) props.put(channelAdIdName, channelType + "$" + channelAdId + "")
                  if (channelKeywordId != 0L) props.put(channelKeywordIdName, channelType + "$" + channelKeywordId + "")


                  if ("true".equals(openToufangLog)) {
                    println("print:LidAndUserFirstEndService web+app 添加事件属性,投放链接:" + lastLid)
                  }
                }

              }
              // TODO 添加用户属性
              if ("usr".equals(dataItem.get("dt").asInstanceOf[String])) {
                val usrPr = dataItem.get("pr").asInstanceOf[util.Map[String, Object]]
                thisZgdid = if (usrPr.get("$zg_did") == null) -1 else usrPr.get("$zg_did").asInstanceOf[Long]
                thisCt = if (usrPr.get("$ct") == null) -1 else usrPr.get("$ct").asInstanceOf[Long]

                zgid = usrPr.get("$zg_zgid").asInstanceOf[Long]
                //获取用户首次 后续 lid lname

                if (StringUtils.isEmpty(lidLnameJsonObj.getString("firstLname"))) {
                  val tuple = getUserLastLidLname(appId, zgid, lidLnameJsonObj, updateSsdbMaxCtMap, updateLIdLnameMap, thisCt)
                  lastLid = tuple._1
                  admsJson = tuple._2
                }

                val firstLname = lidLnameJsonObj.getString("firstLname")
                if (!StringUtils.isEmpty(firstLname)) {
                  hasAddUsrPr = true
                  usrPr.put(userFirstLidName + "lid", lidLnameJsonObj.getLong("firstLid"))
                  usrPr.put(userFirstLidName, firstLname)
                  if ("true".equals(openToufangLog)) {
                    println("print:LidAndUserFirstEndService web+app 在已有的usr下添加用户属性，userFirstPrName：" + userFirstLidName + " ，lid：" + lidLnameJsonObj.getLong("firstLid"))
                  }

                  val folLname = lidLnameJsonObj.getString("folLname")
                  if (!StringUtils.isEmpty(folLname)) {
                    usrPr.put(userFollowLidName + "lid", lidLnameJsonObj.getLong("folLid"));
                    usrPr.put(userFollowLidName, folLname)
                    if ("true".equals(openToufangLog)) {
                      println("print:LidAndUserFirstEndService web+app 在已有的usr下添加用户属性，userFolPrName：" + userFollowLidName + " ，lid：" + lidLnameJsonObj.getLong("folLid"))
                    }

                  }
                }

              }
              //TODO 会话事件匹配上app端广告信息，变更后需更新用户属性
              if ("ss".equals(dataItem.get("dt").asInstanceOf[String]) || "adtf".equals(dataItem.get("dt").asInstanceOf[String])) {
                val usrPr = dataItem.get("pr").asInstanceOf[util.Map[String, Object]]
                thisZgdid = if (usrPr.get("$zg_did") == null) -1 else usrPr.get("$zg_did").asInstanceOf[Long]
                thisCt = if (usrPr.get("$ct") == null) -1 else usrPr.get("$ct").asInstanceOf[Long]
                zgid = usrPr.get("$zg_zgid").asInstanceOf[Long]
                //获取用户首次 后续 lid lname
                if (updateForUserPrKeySet.contains(s"${appId}:${zgid}") && StringUtils.isEmpty(lidLnameJsonObj.getString("firstLname"))) {
                  val tuple = getUserLastLidLname(appId, zgid, lidLnameJsonObj, updateSsdbMaxCtMap, updateLIdLnameMap, thisCt)
                  lastLid = tuple._1
                  admsJson = tuple._2
                }
              }
            }

            //TODO 手动添加用户属性
            if (!hasAddUsrPr && updateForUserPrKeySet.contains(s"${appId}:${zgid}")) {
              //添加了事件属性未添加用户属性 ,需 手动添加dt=usr
              //先获取 $zg_uid
              val zgUid = RedisClient.getHashStr(s"zu:${appId}", zgid + "")
              if ("true".equals(openToufangLog)) {
                println("print:LidAndUserFirstEndService 添加用户属性 获取 " + s"key: zu:${appId} hashKey: ${zgid}" + " 的 uid：" + zgUid)
              }

              var map = new util.HashMap[String, Object]()
              //$zg_did $zg_zgid $ct
              map.put("dt", "usr")
              val jsonObj = new JSONObject()
              if (!StringUtils.isEmpty(zgUid)) {
                jsonObj.put("$zg_uid", zgUid.toLong)
              }
              jsonObj.put("$tz", tz)
              jsonObj.put("$zg_zgid", zgid)
              if (thisZgdid != -1) {
                jsonObj.put("$zg_did", thisZgdid)
              }
              if (thisCt != -1) {
                jsonObj.put("$ct", thisCt)
              }
              val firstLname = lidLnameJsonObj.getString("firstLname")


              if (!StringUtils.isEmpty(firstLname)) {
                jsonObj.put(userFirstLidName + "lid", lidLnameJsonObj.getLong("firstLid"))
                jsonObj.put(userFirstLidName, firstLname)
                if ("true".equals(openToufangLog)) {
                  println("print:LidAndUserFirstEndService web+app 在新增的usr下添加用户属性，userFirstPrName：" + userFirstLidName + " ，lid：" + lidLnameJsonObj.getLong("firstLid"))
                }

                val folLname = lidLnameJsonObj.getString("folLname")
                if (!StringUtils.isEmpty(folLname)) {
                  jsonObj.put(userFollowLidName + "lid", lidLnameJsonObj.getLong("folLid"));
                  jsonObj.put(userFollowLidName, folLname)
                  if ("true".equals(openToufangLog)) {
                    println("print:LidAndUserFirstEndService web+app 在新增的usr下添加用户属性，userFolPrName：" + userFollowLidName + " ，lid：" + lidLnameJsonObj.getLong("folLid"))
                  }

                }
              }
              map.put("pr", jsonObj)
              dataList.add(map)
            }
          }
        }
      } catch {
        case e: Exception =>
          e.printStackTrace()
          logger.error("异常的消息为:---------> " + msg.toString)
      }
    }
    )
  }

  /**
    * 获取用户首次和后续lid lname
    */
  private def getUserLastLidLname(appId: Integer, zgid: Long, jsonObj: JSONObject, updateSsdbMaxCtMap: mutable.HashMap[String, String], updateLIdLnameMap: mutable.HashMap[String, String], ct: Long): Tuple2[Integer, JSONObject] = {
    var lastLid = -1
    val ctKey = s"${ssdbCtPre}:${appId}:${zgid}"
    if ("true".equals(openToufangLog)) {
      println("print:LidAndUserFirstEndService ctKey：" + s"${ssdbCtPre}:${appId}:${zgid}")
    }

    val dataPreKey = s"${ssdbAdDataPre}:${appId}:${zgid}"
    var lidLnames = ""
    var maxCt = 0L
    if (updateSsdbMaxCtMap.contains(ctKey)) {
      val maxCtStr = updateSsdbMaxCtMap(ctKey)
      if (!StringUtils.isEmpty(maxCtStr)) {
        maxCt = maxCtStr.toLong
        lidLnames = updateLIdLnameMap(s"${ctKey}:${maxCtStr}")
      }
    }
    if (!StringUtils.isEmpty(lidLnames)) {
      //添加用户属性 首次（lid lname） 后续（lid lname）
      val lidLname = lidLnames.split("::")
      val arrOne = lidLname(0).split(",")
      val first = arrOne(0).toInt
      jsonObj.put("firstLid", first)
      jsonObj.put("firstLname", arrOne(1))
      lastLid = first
      if (lidLname.size >= 2) {
        val arrTwo = lidLname(1).split(",")
        val last = arrTwo(0).toInt
        jsonObj.put("folLid", last)
        jsonObj.put("folLname", arrTwo(1))
        lastLid = last
      }
    }
    // 事件时间需大于广告时间  匹配事件不设窗口期 && ((ct - maxCt) <= expireTime * 1000)
    if (ct > 0 && ct >= maxCt && lastLid != -1) {
      //      if ("true".equals(openToufangLog)) {
      //        println("print:LidAndUserFirstEndService ctKey的最近一次广告点击 在有效窗口期内：" + ctKey)
      //      }

      //TODO 因明细数据量较大故单条获取
      val adms = AdRedisClient.getStr(s"${dataPreKey}:${maxCt}")
      if ("true".equals(openToufangLog)) {
        println("print:LidAndUserFirstEndService 广告数据key" + s"${dataPreKey}:${maxCt}" + " 获取的广告数据adms：" + adms)
      }

      return Tuple2.apply(lastLid, JSON.parseObject(adms))
    } else {
      if ("true".equals(openToufangLog)) {
        println("print:LidAndUserFirstEndService 事件时间早于广告时间/没有广告 ct ：" + ct + " ,maxCt:" + maxCt)
      }

      return Tuple2.apply(-1, null)
    }

  }

  /**
    * 获取web端 app端对应的广告数据: zgid
    */
  private def setAdMaps(msgs: ListBuffer[ZGMessage], thisMaxCtKeySet: mutable.HashSet[String], thisMaxCtMap: mutable.HashMap[String, Long], thisDataMap: mutable.HashMap[String, String], thisLIdLnameMap: mutable.HashMap[String, String], thisAppSsdbMaxCtMap: mutable.HashMap[String, String], thisAppSsdbDataMap: mutable.HashMap[String, String]) = {
    var jedis = AdRedisClient.jedisPool.getResource
    val pipeline = jedis.pipelined()
    val thisDelKeySet = mutable.HashSet[String]()
    msgs.foreach(msg => {
      if (msg.result != -1) {
        //   msg.plat = msg.data.get("plat").asInstanceOf[Integer]
        val ak = msg.data.get("ak").asInstanceOf[String]
        val pl = msg.data.get("pl").asInstanceOf[String]
        val sdk = msg.data.get("sdk").asInstanceOf[String]
        val appId = msg.appId


        //处理 来自web端+APP端的 广告投放数据，添加用户属性（首次投放链接、后续投放链接）、事件属性（投放链接id lid）
        if (FrontCache.openAdvertisingFunctionAppMap.contains(ak)) {
          val dataItemIterator = msg.data.get("data").asInstanceOf[util.List[util.Map[String, Object]]].iterator()
          while (dataItemIterator.hasNext) {
            val dataItem: util.Map[String, Object] = dataItemIterator.next()
            val dt = dataItem.get("dt").asInstanceOf[String]

            if ("usr".equals(dt) || "evt".equals(dt)) {
              val props = dataItem.get("pr").asInstanceOf[util.Map[String, Object]]
              val zgid = props.get("$zg_zgid").asInstanceOf[Long]
              val appZgidKey = s"${appId}:${zgid}"
              thisMaxCtKeySet += s"${ssdbCtPre}:${appZgidKey}"
            }

            // TODO web端 +苹果ASA 广告信息
            if ("adtf".equals(dt) && !sdk.equals("zg_adtoufang")) {
              val props = dataItem.get("pr").asInstanceOf[util.Map[String, Object]]
              var ct: Long = 0
              if (null != props.get("$ct")) {
                ct = String.valueOf(props.get("$ct")).toLong
              }
              val webAd = if (props.get("$landing_url") == null) "" else props.get("$landing_url").asInstanceOf[String]
              val appleAsaAd = if (props.get("$apple_ad") == null) "" else props.get("$apple_ad").asInstanceOf[String]
              val appleAdChannel = if (props.get("$channel_type") == null) -1 else props.get("$channel_type").asInstanceOf[Integer]

              val zgid = props.get("$zg_zgid").asInstanceOf[Long]
              val appZgidKey = s"${appId}:${zgid}"
              var adMap: util.Map[String, Object] = new util.HashMap[String, Object]()
              //web端广告 唯一点击标识字段：bd_vid(百度信息流,百度搜索) clickid(巨量引擎)  qz_gdt（腾讯广告 非微信流量） gdt_vid（腾讯广告 微信流量）
              if (webAd.contains("lid")) {
                adMap = ToolUtil.urlParseToMap(webAd)
              }

              //苹果ASA广告
              if (appleAdChannel == 5) {
                var appleAdJson = new JSONObject()
                if (appleAsaAd.contains("iad-")) {
                  //10.0以上到14.3 的苹果广告数据需进行转换
                  appleAdJson = appleAdDataTransfer(appleAsaAd)
                } else {
                  try {
                    appleAdJson = JSON.parseObject(appleAsaAd)
                  } catch {
                    case e: Exception => {
                      logger.error("$apple_ad json解析异常")
                    }
                  }
                }
                if (appleAdJson.size() > 0) {
                  adMap = appleASAFeildsMap(appleAdJson)

                  val adClickDate = ToolUtil.dateUsStrToTimestamp(appleAdJson.getString("clickDate"))
                  if (adClickDate > 0L) {
                    ct = adClickDate
                  }
                }
              }

              if (adMap.size() > 0) {
                val channelType = String.valueOf(adMap.get("channel_type")).toInt
                var hashAdData = false
                var isTengXunWeb = false
                if (channelType == 3) {
                  //腾讯web需要单独匹配广告信息 qz_gdt gdt_vid
                  if (adMap.containsKey("qz_gdt") || adMap.containsKey("gdt_vid")) {
                    var muidProcess = if (adMap.containsKey("qz_gdt")) adMap.get("qz_gdt") else adMap.get("gdt_vid")
                    val maxCt = jedis.get(s"${appPre}:${appId}:${muidProcess}")
                    if (StringUtils.isNotEmpty(maxCt)) {
                      val admsJsonStr = jedis.get(s"${appPre}:${appId}:${muidProcess}:${maxCt}")
                      if (StringUtils.isNotEmpty(admsJsonStr)) {

                        thisMaxCtKeySet += s"${ssdbCtPre}:${appZgidKey}"

                        //json转map
                        adMap = JsonUtil.mapFromJson(admsJsonStr)
                        hashAdData = true
                        isTengXunWeb = true
                        //TODO 删除已匹配上广告的 腾讯web广告点击 （每一条广告只能对应一条腾讯web广告点击）

                        val adms = JSON.parseObject(admsJsonStr)
                        val ipUaKey = adms.getString("ip_ua_key")
                        val channelClickIdKey = adms.getString("channel_click_id_key")
                        val appMaxCt = adms.getString("click_time")
                        val appZgidCtKey = s"${appId}:${zgid}:${appMaxCt}"
                        if (StringUtils.isNotEmpty(ipUaKey)) {
                          thisDelKeySet += ipUaKey
                          thisDelKeySet += s"${ipUaKey}:${appMaxCt}"
                        }
                        if (StringUtils.isNotEmpty(channelClickIdKey)) {
                          thisDelKeySet += channelClickIdKey
                          thisDelKeySet += s"${channelClickIdKey}:${appMaxCt}"
                        }
                        adms.put("zuge_ct_key", s"${ssdbCtPre}:${appZgidKey}")
                        adms.put("zuge_data_key", s"${ssdbAdDataPre}:${appZgidCtKey}")
                        adms.put("is_delete", "true")
                        adms.put("other_key", s"${appPre}:${appId}:${muidProcess}:${maxCt}") //匹配的key
                        val jsonObj = new JSONObject()
                        jsonObj.put("tableName", "toufang_ad_click")
                        jsonObj.put("sinkType", "kudu")
                        jsonObj.put("data", adms.toJSONString)
                        if (!MainService.test) {
                          DistributeService.simpleSend(new ProducerRecord[String, String](DistributeService.typeTopicMap("toufang_kudu"), s"${appId}:${ipUaKey}", jsonObj.toJSONString.replace("\\\"", "\"").replace("\"{", "{").replace("}\"", "}")))
                        }
                      }
                    } else {
                      //腾讯web端广告数据未匹配上的先存redis，设置60s过期时间
                      pipeline.set(s"${appPre}:${muidProcess}:rawdata", msg.rawData)
                      pipeline.expire(s"${appPre}:${muidProcess}:rawdata", tengxunWebExpireTime)
                      if ("true".equals(openToufangLog)) {
                        println("print:LidAndUserFirstEndService 腾讯web未匹配上广告:" + s"${appPre}:${muidProcess}:rawdata")
                      }
                    }
                  }
                } else {
                  thisMaxCtKeySet += s"${ssdbCtPre}:${appZgidKey}"
                  hashAdData = true
                }


                if (adMap.containsKey("lid") && hashAdData) {
                  val lid = String.valueOf(adMap.get("lid"))
                  val lname = String.valueOf(adMap.get("lname"))

                  val appZgidCtKey = s"${appId}:${zgid}:${ct}"
                  if (!thisMaxCtMap.contains(appZgidKey) || ct >= thisMaxCtMap.get(appZgidKey).get) {
                    val adms = new AdMessage()
                    adms.zg_appid = appId.toLong
                    adms.lid = lid.toLong
                    adms.ct = ct
                    adms.app_key = ak
                    adms.setFeildsWithout(adMap)
                    //百度和巨量的 需要单独将 $landing_url 赋值给 callback (腾讯的用lua脚本解析的广告)
                    if (!isTengXunWeb) {
                      adms.callback_url = webAd
                    }
                    if (adMap.containsKey("baidu_token")) {
                      adms.token = String.valueOf(adMap.get("baidu_token"))
                    }


                    thisMaxCtMap.put(appZgidKey, ct)
                    thisDataMap.put(appZgidCtKey, adms.toJsonString())
                    if ("true".equals(openToufangLog)) {
                      println("print:LidAndUserFirstEndService web端 +苹果ASA 放入thisLIdLnameMap:" + appZgidCtKey + "value:" + s"${lid},${lname}")
                    }
                    thisLIdLnameMap.put(appZgidCtKey, s"${lid},${lname}")
                  }
                }
              }
            }

            //TODO app端广告信息
            if ("ss".equals(dt)) {
              val props = dataItem.get("pr").asInstanceOf[util.Map[String, Object]]
              val zgid = String.valueOf(props.get("$zg_zgid")).toLong
              val ip: String = msg.data.get("ip").asInstanceOf[String]
              val ua: String = msg.data.get("ua").asInstanceOf[String]

              val os = String.valueOf(if (props.get("$os") == null) "" else props.get("$os"))
              val ov = String.valueOf(if (props.get("$ov") == null) "" else props.get("$ov"))

              var appProcessKey = ""
              var appMaxCt = -1L
              // TODO 精确匹配 （按匹配顺序匹配，不考虑广告时间）
              val idfaOrigin = String.valueOf(if (props.get("$idfa") == null) "" else props.get("$idfa"))
              val imeiOrigin = String.valueOf(if (props.get("$imei") == null) "" else props.get("$imei"))
              val androidIdOrigin = String.valueOf(if (props.get("$android_id") == null) "" else props.get("$android_id"))
              val oaidOrigin = String.valueOf(if (props.get("$oaid") == null) "" else props.get("$oaid"))


              val idfa = if (originExcludeSet.contains(idfaOrigin) || md5ExcludeSet.contains(idfaOrigin)) "" else (if (idfaOrigin.length == 32) idfaOrigin else ToolUtil.getMD5Str(idfaOrigin))
              val imei = if (originExcludeSet.contains(imeiOrigin) || md5ExcludeSet.contains(imeiOrigin)) "" else (if (imeiOrigin.length == 32) imeiOrigin else ToolUtil.getMD5Str(imeiOrigin))
              val androidId = if (originExcludeSet.contains(androidIdOrigin) || md5ExcludeSet.contains(androidIdOrigin)) "" else (if (androidIdOrigin.length == 32) androidIdOrigin else ToolUtil.getMD5Str(androidIdOrigin))
              val oaid = if (originExcludeSet.contains(oaidOrigin) || md5ExcludeSet.contains(oaidOrigin)) "" else (if (oaidOrigin.length == 32) oaidOrigin else ToolUtil.getMD5Str(oaidOrigin))
              if ("iOS".equals(os)) {
                //IOS系统: muid
                if (StringUtils.isNotEmpty(idfa)) {
                  val processKey = s"${appPre}:${appId}:${idfa}"
                  if (thisAppSsdbMaxCtMap.contains(processKey) && !thisDelKeySet.contains(processKey)) {
                    var maxCt = thisAppSsdbMaxCtMap.get(processKey).get.toLong
                    if (maxCt > appMaxCt) {
                      appMaxCt = maxCt
                      appProcessKey = processKey
                    }
                  }
                }
              } else {
                var hasAndriodDevice = false
                //安卓系统：调整顺序 oaid>android_id>imei
                if (StringUtils.isNotEmpty(oaid) && !hasAndriodDevice) {
                  val processKey = s"${appPre}:${appId}:${oaid}"
                  if (thisAppSsdbMaxCtMap.contains(processKey) && !thisDelKeySet.contains(processKey)) {
                    var maxCt = thisAppSsdbMaxCtMap.get(processKey).get.toLong
                    if (maxCt > appMaxCt) {
                      appMaxCt = maxCt
                      appProcessKey = processKey
                      hasAndriodDevice = true
                    }
                  }
                }

                if (StringUtils.isNotEmpty(androidId) && !hasAndriodDevice) {
                  val processKey = s"${appPre}:${appId}:${androidId}"
                  if (thisAppSsdbMaxCtMap.contains(processKey) && !thisDelKeySet.contains(processKey)) {
                    var maxCt = thisAppSsdbMaxCtMap.get(processKey).get.toLong
                    if (maxCt > appMaxCt) {
                      appMaxCt = maxCt
                      appProcessKey = processKey
                      hasAndriodDevice = true
                    }
                  }
                }
                if (StringUtils.isNotEmpty(imei) && !hasAndriodDevice) {
                  val processKey = s"${appPre}:${appId}:${imei}"
                  if (thisAppSsdbMaxCtMap.contains(processKey) && !thisDelKeySet.contains(processKey)) {
                    var maxCt = thisAppSsdbMaxCtMap.get(processKey).get.toLong
                    if (maxCt > appMaxCt) {
                      appMaxCt = maxCt
                      appProcessKey = processKey
                      hasAndriodDevice = true
                    }
                  }
                }
              }
              if (StringUtils.isEmpty(appProcessKey)) {
                // TODO 模糊匹配（按就近的广告时间匹配）
                var uaProcess = ToolUtil.uaAnalysis(ua)
                var ipUaKey = s"${appPre}:${appId}:${ip}${uaProcess}"
                if ("true".equals(openToufangLog)) {
                  println("print:LidAndUserFirstEndService thisAppSsdbMaxCtMap:" + thisAppSsdbMaxCtMap)
                }

                if (thisAppSsdbMaxCtMap.contains(ipUaKey) && !thisDelKeySet.contains(ipUaKey)) {
                  appMaxCt = thisAppSsdbMaxCtMap.get(ipUaKey).get.toLong
                  appProcessKey = ipUaKey
                }
                // TODO iOS 的广告 需同时匹配 ip+version
                if ("iOS".equals(os)) {
                  val arr = uaProcess.split(":")
                  if (arr.size > 0) {
                    var version = ":" + arr(1)
                    var ipVersionKey = s"${appPre}:${appId}:${ip}${version}"
                    if (thisAppSsdbMaxCtMap.contains(ipVersionKey) && !thisDelKeySet.contains(ipVersionKey)) {
                      var maxCt = thisAppSsdbMaxCtMap.get(ipVersionKey).get.toLong
                      if (maxCt > appMaxCt) {
                        appMaxCt = maxCt
                        appProcessKey = ipVersionKey
                      }
                    }
                  }
                }
              }


              if (appMaxCt != -1L) {
                val appDataKey = s"${appProcessKey}:${appMaxCt}"
                if (thisAppSsdbDataMap.contains(appDataKey)) {
                  // TODO 对于未匹配上ss的广告才参与匹配（上面已做判断）

                  val appZgidKey = s"${appId}:${zgid}"
                  val appZgidCtKey = s"${appId}:${zgid}:${appMaxCt}"


                  thisMaxCtKeySet += s"${ssdbCtPre}:${appZgidKey}"

                  val admsJsonStr = thisAppSsdbDataMap.get(appDataKey).get
                  val adms = JSON.parseObject(admsJsonStr)
                  //TODO 删除已匹配上ss的广告 （每一条广告只能对应一条ss事件）
                  val ipUaKey = adms.getString("ip_ua_key")
                  val muidKey = adms.getString("muid_key")
                  val idfaKey = adms.getString("idfa_key")
                  val imeiKey = adms.getString("imei_key")
                  val androidIdKey = adms.getString("android_id_key")
                  val oaidKey = adms.getString("oaid_key")
                  //idfa_key  imei_key  android_id_key oaid_key channel_click_id_key

                  if (StringUtils.isNotEmpty(ipUaKey)) {
                    thisDelKeySet += ipUaKey
                    thisDelKeySet += s"${ipUaKey}:${appMaxCt}"
                  }
                  if (StringUtils.isNotEmpty(muidKey)) {
                    thisDelKeySet += muidKey
                    thisDelKeySet += s"${muidKey}:${appMaxCt}"
                  }
                  if (StringUtils.isNotEmpty(idfaKey)) {
                    thisDelKeySet += idfaKey
                    thisDelKeySet += s"${idfaKey}:${appMaxCt}"
                  }
                  if (StringUtils.isNotEmpty(imeiKey)) {
                    thisDelKeySet += imeiKey
                    thisDelKeySet += s"${imeiKey}:${appMaxCt}"
                  }
                  if (StringUtils.isNotEmpty(androidIdKey)) {
                    thisDelKeySet += androidIdKey
                    thisDelKeySet += s"${androidIdKey}:${appMaxCt}"
                  }
                  if (StringUtils.isNotEmpty(oaidKey)) {
                    thisDelKeySet += oaidKey
                    thisDelKeySet += s"${oaidKey}:${appMaxCt}"
                  }
                  adms.put("zuge_ct_key", s"${ssdbCtPre}:${appZgidKey}")
                  adms.put("zuge_data_key", s"${ssdbAdDataPre}:${appZgidCtKey}")
                  adms.put("is_delete", "true")
                  adms.put("other_key", appDataKey) //匹配的key
                  val jsonObj = new JSONObject()
                  jsonObj.put("tableName", "toufang_ad_click")
                  jsonObj.put("sinkType", "kudu")
                  jsonObj.put("data", adms.toJSONString)
                  if (!MainService.test) {
                    DistributeService.simpleSend(new ProducerRecord[String, String](DistributeService.typeTopicMap("toufang_kudu"), s"${appId}:${ipUaKey}", jsonObj.toJSONString.replace("\\\"", "\"").replace("\"{", "{").replace("}\"", "}")))
                  }


                  val lid = String.valueOf(adms.get("lid")).toLong
                  val lname = String.valueOf(adms.get("lname"))

                  if (!thisMaxCtMap.contains(appZgidKey) || appMaxCt >= thisMaxCtMap.get(appZgidKey).get) {

                    thisMaxCtMap.put(appZgidKey, appMaxCt)
                    thisDataMap.put(appZgidCtKey, admsJsonStr)
                    if ("true".equals(openToufangLog)) {
                      println("print:LidAndUserFirstEndService app端ss事件 放入thisLIdLnameMap:" + appZgidCtKey + "value:" + s"${lid},${lname}")
                    }
                    thisLIdLnameMap.put(appZgidCtKey, s"${lid},${lname}")
                  }

                }
              }
            }
          }
        }
      }
    })
    //删除已匹配上ss的过期广告数据
    if (thisDelKeySet.size > 0) {
      if ("true".equals(openToufangLog)) {
        println("print:LidAndUserFirstEndService 删除已匹配上ss的广告数据thisDelKeySet:" + thisDelKeySet)
      }

      jedis.del(thisDelKeySet.toArray: _*)
    }
    pipeline.sync()
    if (jedis != null) {
      jedis.close()
    }
  }

  /**
    * 获取 ip+ua
    */
  private def setAppIpUaKeys(msgs: ListBuffer[ZGMessage], thisAppMaxCtKeySet: mutable.HashSet[String]) = {
    msgs.foreach(msg => {
      if (msg.result != -1) {
        val ak = msg.data.get("ak").asInstanceOf[String]
        val appId = msg.appId
        if (FrontCache.openAdvertisingFunctionAppMap.contains(ak)) {
          val iterator = msg.data.get("data").asInstanceOf[util.List[util.Map[String, Object]]].iterator()

          while (iterator.hasNext) {
            val dataItem = iterator.next()
            val dt = String.valueOf(dataItem.get("dt"))
            val ip: String = msg.data.get("ip").asInstanceOf[String]
            val ua: String = msg.data.get("ua").asInstanceOf[String]

            // 判断"dt"是否有会话开始事件ss
            if ("ss".equals(dt) && !StringUtils.isEmpty(ip)) {
              val props = dataItem.get("pr").asInstanceOf[util.Map[String, Object]]
              val os = String.valueOf(if (props.get("$os") == null) "" else props.get("$os"))
              val ov = String.valueOf(if (props.get("$ov") == null) "" else props.get("$ov"))

              //TODO 模糊匹配：iphone做特殊解析
              var uaProcess = ToolUtil.uaAnalysis(ua)
              val ipUaKey = s"${appPre}:${appId}:${ip}${uaProcess}"
              thisAppMaxCtKeySet += ipUaKey
              // TODO iOS 的广告 需同时匹配 ip+version
              if ("iOS".equals(os)) {
                val arr = uaProcess.split(":")
                if (arr.size > 0) {
                  var version = ":" + arr(1)
                  val ipVersionKey = s"${appPre}:${appId}:${ip}${version}"
                  thisAppMaxCtKeySet += ipVersionKey
                }
              }

              //TODO 精确匹配
              val idfaOrigin = String.valueOf(if (props.get("$idfa") == null) "" else props.get("$idfa"))
              val imeiOrigin = String.valueOf(if (props.get("$imei") == null) "" else props.get("$imei"))
              val androidIdOrigin = String.valueOf(if (props.get("$android_id") == null) "" else props.get("$android_id"))
              val oaidOrigin = String.valueOf(if (props.get("$oaid") == null) "" else props.get("$oaid"))
              val qaidCaaOrigin = String.valueOf(if (props.get("$qaid_caa") == null) "" else props.get("$qaid_caa"))

              val idfa = if (originExcludeSet.contains(idfaOrigin) || md5ExcludeSet.contains(idfaOrigin)) "" else (if (idfaOrigin.length == 32) idfaOrigin else ToolUtil.getMD5Str(idfaOrigin))
              val imei = if (originExcludeSet.contains(imeiOrigin) || md5ExcludeSet.contains(imeiOrigin)) "" else (if (imeiOrigin.length == 32) imeiOrigin else ToolUtil.getMD5Str(imeiOrigin))
              val androidId = if (originExcludeSet.contains(androidIdOrigin) || md5ExcludeSet.contains(androidIdOrigin)) "" else (if (androidIdOrigin.length == 32) androidIdOrigin else ToolUtil.getMD5Str(androidIdOrigin))
              val oaid = if (originExcludeSet.contains(oaidOrigin) || md5ExcludeSet.contains(oaidOrigin)) "" else (if (oaidOrigin.length == 32) oaidOrigin else ToolUtil.getMD5Str(oaidOrigin))
              val qaidCaa = if (originExcludeSet.contains(qaidCaaOrigin) || md5ExcludeSet.contains(qaidCaaOrigin)) "" else (if (qaidCaaOrigin.length == 32) qaidCaaOrigin else ToolUtil.getMD5Str(qaidCaaOrigin))

              if (StringUtils.isNotEmpty(idfa)) {
                val muidProcessKey = s"${appPre}:${appId}:${idfa}"
                thisAppMaxCtKeySet += muidProcessKey
              }
              if (StringUtils.isNotEmpty(imei)) {
                val muidProcessKey = s"${appPre}:${appId}:${imei}"
                thisAppMaxCtKeySet += muidProcessKey
              }
              if (StringUtils.isNotEmpty(androidId)) {
                val muidProcessKey = s"${appPre}:${appId}:${androidId}"
                thisAppMaxCtKeySet += muidProcessKey
              }
              if (StringUtils.isNotEmpty(oaid)) {
                val muidProcessKey = s"${appPre}:${appId}:${oaid}"
                thisAppMaxCtKeySet += muidProcessKey
              }
              if (StringUtils.isNotEmpty(qaidCaa)) {
                val muidProcessKey = s"${appPre}:${appId}:${qaidCaa}"
                thisAppMaxCtKeySet += muidProcessKey
              }
            }
          }
        }
      }
    })
  }

  /**
    * 操作系统映射
    */
  def getOsMaping(os: String): String = {
    //android 0 、 ios 1 、其他 3
    var osProcess = ""
    if (os.toLowerCase.equals("ios")) {
      osProcess = "1"
    } else if (os.toLowerCase.equals("android")) {
      osProcess = "0"
    } else if (!StringUtils.isEmpty(os)) {
      osProcess = "3"
    }
    return osProcess
  }

  /**
    * 苹果ASA广告信息字段映射
    */
  def appleASAFeildsMap(json: JSONObject): java.util.Map[String, java.lang.Object] = {
    var map: util.Map[String, Object] = new util.HashMap[String, Object]()
    map.put("lname", "苹果ASA")
    map.put("lid", "-2")
    map.put("channel_type", "5")
    map.put("channel_account_id", String.valueOf(json.getLongValue("orgId")))
    map.put("channel_campaign_id", String.valueOf(json.getLongValue("campaignId")))
    map.put("channel_adgroup_id", String.valueOf(json.getLongValue("adGroupId")))
    map.put("channel_ad_id", String.valueOf(if (json.getLongValue("adId") == 0L) json.getLongValue("creativeSetId") else json.getLongValue("adId")))
    map.put("channel_keyword_id", String.valueOf(json.getLongValue("keywordId")))
    return map
  }

  /**
    * 苹果ASA 10.0以上到14.3 转换为 14.3以上广告格式
    */
  def appleAdDataTransfer(adStr: String): JSONObject = {
    val json = new JSONObject()
    if (adStr.contains("=")) {
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
    } else {
      try {
        val jsonObj = JSON.parseObject(adStr)
        json.put("orgId", jsonObj.getLongValue("iad-org-id"))
        json.put("campaignId", jsonObj.getLongValue("iad-campaign-id"))
        json.put("adGroupId", jsonObj.getLongValue("iad-adgroup-id"))
        json.put("keywordId", jsonObj.getLongValue("iad-keyword-id"))
        json.put("adId", jsonObj.getLongValue("iad-ad-id"))
        json.put("clickDate", jsonObj.getString("iad-click-date"))
      } catch {
        case e: Exception => {
          logger.error("$apple_ad json解析异常")
        }
      }
    }

    return json
  }

  /**
    * 广告渠道名称映射：$channel_type：百度信息流 1、巨量引擎 2、腾讯广告 3、百度搜索 4，苹果ASA 5，知乎 9，快手 10、 微博-超级粉丝通 11、华为广告 12、小米营销 13、VIVO营销平台 14、OPPO营销平台 15
    */
  def getchannelTypeNameStr(channelType: String): String = {
    if ("1".equals(channelType)) {
      return "百度信息流"
    } else if ("2".equals(channelType)) {
      return "巨量引擎"
    } else if ("3".equals(channelType)) {
      return "腾讯广告"
    } else if ("4".equals(channelType)) {
      return "百度搜索"
    } else if ("5".equals(channelType)) {
      return "苹果ASA"
    } else if ("9".equals(channelType)) {
      return "知乎营销"
    } else if ("10".equals(channelType)) {
      return "快手广告"
    } else if ("11".equals(channelType)) {
      return "微博-超级粉丝通"
    } else if ("12".equals(channelType)) {
      return "华为广告"
    } else if ("13".equals(channelType)) {
      return "小米营销"
    } else if ("14".equals(channelType)) {
      return "VIVO营销平台"
    } else if ("15".equals(channelType)) {
      return "OPPO营销平台"
    }
    return ""
  }
}
