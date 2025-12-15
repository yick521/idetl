package com.zhugeio.etl.id.adtoufang.service

import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}
import com.zhugeio.etl.adtoufang.common.OperatorUtil
import com.zhugeio.etl.id.ZGMessage
import com.zhugeio.etl.id.adtoufang.common.{ConvertMessage, ConvertMessageV2}
import com.zhugeio.etl.id.adtoufang.service.AdService.appPre
import com.zhugeio.etl.id.adtoufang.service.LidAndUserFirstEndService.convertExpireTime
import com.zhugeio.etl.id.cache.FrontCache
import com.zhugeio.etl.id.cache.FrontCache.adsLinkEventMap
import com.zhugeio.etl.id.dao.FrontDao
import com.zhugeio.etl.id.redis.AdRedisClient
import com.zhugeio.etl.id.service.MainService.openToufangLog
import com.zhugeio.etl.id.service.{DistributeService, MainService}
import org.apache.commons.lang.StringUtils
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.log4j.Logger

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util
import scala.collection.mutable
import scala.collection.mutable.ListBuffer

object ConvertEventService {
  val logger = Logger.getLogger(this.getClass)
  val EVT = "evt" //回传事件仅针对于全埋点
  val formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
  val utmPre = "utm"
  val adConvertPrefix = "ad:convert:status:"

  def convertMessage(msgs: ListBuffer[ZGMessage]): Unit = {
    msgs.filter(_.result != -1).foreach(msg => {

      val ak = msg.data.get("ak").asInstanceOf[String]
      val pl = msg.data.get("pl").asInstanceOf[String]
      //var plat=msg.data.get("plat").asInstanceOf[Integer]
      val appId = msg.appId
      //处理 来自web端的（"pl"="js"）广告投放应用，添加用户属性（首次投放链接、后续投放链接）、事件属性（投放链接id lid）
      if (FrontCache.openAdvertisingFunctionAppMap.contains(ak)) {
        val dataItemIterator = msg.data.get("data").asInstanceOf[java.util.List[java.util.Map[String, java.lang.Object]]].iterator()
        while (dataItemIterator.hasNext) {
          val dataItem: util.Map[String, Object] = dataItemIterator.next()
          //处理事件属性 lid bd_vid
          if ("evt".equals(dataItem.get("dt").asInstanceOf[String])) {
            val props = dataItem.get("pr").asInstanceOf[java.util.Map[String, java.lang.Object]]
            val eventName = String.valueOf(props.get("$eid")) //事件名称
            val zgEid = String.valueOf(props.get("$zg_eid")).toInt //事件id
            val zgDid = String.valueOf(props.get("$zg_did")).toInt //设备id
            val zgId = String.valueOf(props.get("$zg_zgid")).toInt //诸葛id
            var ct = String.valueOf(if (props.get("$ct") == null) 0 else props.get("$ct")).toLong //事件时间

            // 获取用户最后一条广告信息
            val maxCt = AdRedisClient.getStr(s"${LidAndUserFirstEndService.ssdbCtPre}:${appId}:${zgId}")
            if (StringUtils.isNotEmpty(maxCt)) {
              //事件事件在窗口期内:30天
              var adMessageJsonStr = AdRedisClient.getStr(s"${LidAndUserFirstEndService.ssdbAdDataPre}:${appId}:${zgId}:${maxCt}")
              if (StringUtils.isNotEmpty(adMessageJsonStr)) {
                if ("true".equals(openToufangLog)) {
                  println(s"print:ConvertEventService 深度回传事件获取到广告信息  adtf:data:${appId}:${zgId}:${maxCt}")
                }

                val adMessageJson = JSON.parseObject(adMessageJsonStr)
                val lid = String.valueOf(adMessageJson.get("lid")).toInt //链接ID
                val lidConvertKey = s"${zgEid}_${lid}"
                if ("true".equals(openToufangLog)) {
                  println(s"print:ConvertEventService 获取深度回传条件lidConvertKey:" + lidConvertKey)
                }
                if (adsLinkEventMap.contains(lidConvertKey)) {
                  val adsLinkEvent = adsLinkEventMap.get(lidConvertKey).get
                  val windowTime = adsLinkEvent.window_time //原为 convertExpireTime
                  if ("true".equals(openToufangLog)) {
                    println(s"print:ConvertEventService 获取时间 ct:" + ct + " maxCt：" + maxCt + " windowTime：" + windowTime + " flag：" + ((ct - maxCt.toLong) <= windowTime * 1000))
                  }
                  if (ct > 0 && ((ct - maxCt.toLong) <= (windowTime * 1000)) && ct >= maxCt.toLong) {
                    //判断首次记录表 ads_frequency_first 不存在转化数据 且 链接事件满足 ads_link_event 表 中的 链接id与深度转换事件 才开始转化流程
                    // TODO 直接从mysql查询

                    //                    var eventContains = false
                    //                    if (FrontCache.appIdSMap.contains(lid)) {
                    //                      eventContains = FrontCache.appIdSMap(lid) == zgEid
                    //                    }
                    var existFrequency = FrontCache.adFrequencySet.contains(s"${zgEid}_${lid}_${zgId}")

                    if ("true".equals(openToufangLog)) {
                      println("print:ConvertEventService 深度回传事件 首次回传表是否存在 existFrequency:" + existFrequency + " ,本次事件zgEid_lid_zgId：" + s"${zgEid}_${lid}_${zgId}")
                    }


                    if (!existFrequency) {
                      if ("true".equals(openToufangLog)) {
                        println("print:ConvertEventService 深度回传事件 zgEid：" + zgEid + " 该事件满足 事件id 转化")
                      }

                      val convertMsgV2 = new ConvertMessageV2() //回传行为数据，发至kafka
                      val adConvertEvent = new ConvertMessage() //回传事件记录信息 写入PG表 toufang_convert_event

                      //判断属性条件 TODO 直接从mysql查询 frequency 字段，不能走缓存(ads_link_event表不是由我们写入)，否则影响数据的准确性
                      //val adsLinkEvent = FrontDao.getConvent(zgEid, lid)
                      var flag = false
                      if ("true".equals(openToufangLog)) {
                        println("print:ConvertEventService matchJson type:从 ads_link_event 表获取回传事件的频次：" + adsLinkEvent.toJsonString())
                      }
                      if (StringUtils.isNotEmpty(adsLinkEvent.match_json) && !"{}".equals(adsLinkEvent.match_json)) {
                        val matchJson = JSON.parseObject(adsLinkEvent.match_json) //判断属性是否符合
                        if ("true".equals(openToufangLog)) {
                          println("print:ConvertEventService matchJson type:" + matchJson.getInteger("type"))
                          println("print:ConvertEventService matchJson operator:" + matchJson.getString("operator"))
                          println("print:ConvertEventService matchJson values:" + matchJson.getJSONArray("values"))
                        }
                        val proIsRight = OperatorUtil.compareProValue(appId, zgId, props, matchJson, zgDid)
                        if (proIsRight) {
                          flag = true
                        }
                      } else {
                        flag = true
                      }
                      //满足事件属性条件
                      if (flag) {
                        var sendFlag = false
                        val eventIdsArr = adsLinkEvent.event_ids.split(",")
                        if (eventIdsArr.size > 1) {
                          //存入ssdb
                          val adConvertStatusKey = s"${adConvertPrefix}:${zgEid}:${lid}"
                          AdRedisClient.setStr(adConvertStatusKey, "1", windowTime.toInt)
                          if ("true".equals(openToufangLog)) {
                            println("print:ConvertEventService 存adConvertStatusKey :" + adConvertStatusKey + " windowTime:" + windowTime)
                          }
                          //TODO 从ssdb获取所有eid的状态
                          val adConvertStatusKeySet = mutable.HashSet[String]()

                          eventIdsArr.foreach(eventId => {
                            adConvertStatusKeySet.add(s"${adConvertPrefix}:${eventId}:${lid}")
                            if ("true".equals(openToufangLog)) {
                              println(s"print:ConvertEventService 获取adConvertStatusKey :${adConvertPrefix}:${eventId}:${lid}")
                            }
                          })
                          val adConvertResult = AdRedisClient.mgetResult(adConvertStatusKeySet, false)
                          if ("true".equals(openToufangLog)) {
                            println("print:ConvertEventService adConvertResult size:" + adConvertResult.size)
                          }
                          sendFlag = (adConvertResult.size == adConvertStatusKeySet.size)
                        } else {
                          sendFlag = true
                        }

                        //TO满足深度回传发kafka
                        if (sendFlag) {
                          //TODO 满足多事件的回传条件
                          if ("true".equals(openToufangLog)) {
                            println("print:ConvertEventService 深度回传事件 zgEid：" + zgEid + " 该事件满足 属性id 转化")
                          }

                          adConvertEvent.match_json = adsLinkEvent.match_json
                          var tmp_callback_url = adMessageJson.get("callback_url")
                          if (tmp_callback_url == null || "null".equals(tmp_callback_url)) {
                            tmp_callback_url = ""
                          }

                          // 写入 ads_frequency_first 表
                          //insert into ads_frequency_first values(5633,179,'',10000000)
                          if (adsLinkEvent.frequency == 0) { //判断频次是首次(0)还是每次(1)
                            //写入 web mysql TODO 这里必须及时写库，否则影响页面数据的准确性（不能由下游写库）
                            FrontDao.insertFrequency(zgEid.toString, lid, zgId, adsLinkEvent.channel_event, formatter.format(new Timestamp(ct))) //首次回传事件记录
                            if ("true".equals(openToufangLog)) {
                              println("print:ConvertEventService 频次为首次（0），写入ads_frequency_first表，lid：" + lid)
                            }

                            FrontCache.adFrequencySet.add(s"${zgEid}_${lid}_${zgId}")
                          }
                          //从 ads_link_toufang 表 获取广告自定义参数
                          val utm = FrontDao.getUTM(lid)
                          if ("true".equals(openToufangLog)) {
                            println("print:ConvertEventService 从 ads_link_toufang 表 获取广告utm信息:" + utm.toJsonString)
                          }

                          if (utm != null) {
                            //select utm_source,utm_medium,utm_campaign,utm_content,utm_term from b_user_event_all_461 limit 5;
                            //将 utm_source,utm_medium,utm_campaign,utm_content,utm_term 写入ssdb ，由dw模块写入kudu
                            var utmKey = s"${utmPre}:${zgEid}"
                            AdRedisClient.setStr(utmKey, utm.toJsonString, 86400) //过期时间设为24小时
                            //赋值
                            adConvertEvent.getUtm(utm)
                          }

                          adConvertEvent.zg_eid = zgEid
                          adConvertEvent.zg_appid = appId
                          adConvertEvent.zg_id = zgId;
                          adConvertEvent.lid = lid
                          if (ct == 0) {
                            ct = System.currentTimeMillis()
                          }
                          adConvertEvent.event_time = ct
                          adConvertEvent.channel_event = adsLinkEvent.channel_event
                          adConvertEvent.channel_id = adMessageJson.getLongValue("channel_id")
                          adConvertEvent.channel_adgroup_id = adMessageJson.getLongValue("channel_adgroup_id")
                          adConvertEvent.channel_adgroup_name = adMessageJson.getString("channel_adgroup_name")
                          adConvertEvent.click_time = adMessageJson.getLongValue("click_time")
                          adConvertEvent.frequency = adsLinkEvent.frequency
                          adConvertEvent.event_name = eventName
                          //将需要落库的数据发kafka
                          val jsonObj = new JSONObject()
                          jsonObj.put("tableName", "toufang_convert_event")
                          jsonObj.put("sinkType", "kudu")
                          jsonObj.put("data", adConvertEvent.toConvertJson())
                          if (!MainService.test) {
                            DistributeService.simpleSend(new ProducerRecord[String, String](DistributeService.typeTopicMap("toufang_kudu"), s"${appId}:${zgId}", jsonObj.toJSONString.replace("\\\"", "\"").replace("\"{", "{").replace("}\"", "}")))
                          }
                          if ("true".equals(openToufangLog)) {
                            println("print:ConvertEventService 转化行为 到 kafka toufang_kudu：" + jsonObj.toJSONString.replace("\\\"", "\"").replace("\"{", "{").replace("}\"", "}"))
                          }

                          //将转化行为数据 存入结果集

                          convertMsgV2.callback_url = String.valueOf(tmp_callback_url)
                          convertMsgV2.lid = lid
                          val eventType = FrontCache.lidAndChannelEventMap(lid + "_" + zgEid)
                          convertMsgV2.event_type = if (eventType == null) "" else eventType
                          convertMsgV2.action_time = ct + ""
                          convertMsgV2.setFeilds(adMessageJson)
                          convertMsgV2.zg_id = zgId
                          //写kafka
                          if (!MainService.test) {
                            DistributeService.simpleSend(new ProducerRecord[String, String](DistributeService.typeTopicMap("toufang"), convertMsgV2.toJsonString()))
                          }
                          if ("true".equals(openToufangLog)) {
                            println("print:ConvertEventService 回传数据 到 kafka toufang_ad_user：" + convertMsgV2.toJsonString())
                          }
                        }

                      }
                    }
                  }
                }

              }

            }
          }
        }
      }
    })
  }
}
