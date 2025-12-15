package com.zhugeio.etl.id.service

import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}
import com.zhugeio.etl.adtoufang.common.OperatorUtil
import com.zhugeio.etl.id.cache.FrontCache
import com.zhugeio.etl.id.log.IdLogger
import com.zhugeio.etl.id.{Config, ZGMessage}
import com.zhugeio.tool.commons.JsonUtil
import org.apache.log4j.Logger
import java.{lang, util}

import com.zhugeio.etl.commons.ErrorMessageEnum

import scala.collection.mutable.ListBuffer

/**
  * Created by ziwudeng on 9/14/16.
  */
object VirtualEventService {

  def handleVirtualEvent(msgs: ListBuffer[ZGMessage]): Unit = {
    var list = new util.ArrayList[ZGMessage]()
    msgs.foreach(msg => {
      if (msg.result != -1) {
        val appId = msg.appId
        if (FrontCache.virtualEventAppidsSet.contains(appId.toString)) {
          val oldOwner = msg.data.get("owner").asInstanceOf[String]
          val dataItemIterator = msg.data.get("data").asInstanceOf[java.util.List[java.util.Map[String, java.lang.Object]]].iterator()
          while (dataItemIterator.hasNext) {
            val dataItem = dataItemIterator.next()
            val dt = String.valueOf(dataItem.get("dt"))
            val owner = EventService.getOwner(dt, oldOwner)
            if ("evt" == dt || "abp" == dt) {
              val pr = dataItem.get("pr").asInstanceOf[java.util.Map[String, java.lang.Object]]
              val eventName = pr.get("$eid").asInstanceOf[String]
              var key = s"${appId}_${owner}_${eventName}"
              if (FrontCache.virtualEventMap.contains(key)) {
                //新增为虚拟事件
                val eventJsonStrArr = FrontCache.virtualEventMap(key)
                eventJsonStrArr.forEach(
                  eventJsonStr => {
                    val eventJsonObj: JSONObject = JSON.parseObject(eventJsonStr)
                    val virtualName = eventJsonObj.getString("virtual_name")
                    val virtualAlias = eventJsonObj.getString("virtual_alias")
                    val filters = eventJsonObj.getJSONObject("filters")
                    var compareResult = false
                    if (filters != null) {
                      val matchJsonArr = filters.getJSONArray("conditions")
                      if (matchJsonArr != null) {
                        compareResult = getCompareResult(dt, pr, matchJsonArr, msg)
                      } else {
                        compareResult = true
                      }
                    } else {
                      compareResult = true
                    }
                    if (compareResult) {
                      //满足虚拟事件的条件-生成一条虚拟事件
                      val jsonStr: String = JsonUtil.toJson(msg.data)
                      val jsonObj: JSONObject = JSON.parseObject(jsonStr)
                      jsonObj.put("owner", "zg_vtl")
                      val dataArr = jsonObj.getJSONArray("data")
                      dataArr.clear();
                      val dataJsonStr: String = JsonUtil.toJson(dataItem)
                      val dataJsonObj: JSONObject = JSON.parseObject(dataJsonStr)
                      dataJsonObj.put("dt", "vtl");
                      val prJson = dataJsonObj.getJSONObject("pr")
                      prJson.put("$eid", virtualName);
                      prJson.put("$virtual_alias", virtualAlias);
                      prJson.put("$event_owner", owner);
                      prJson.put("$event_name", eventName);
                      prJson.put("$event_dt", dt);
                      dataArr.add(dataJsonObj)
                      val json = jsonObj.toJSONString
                      val virtualMessage = new ZGMessage("", 0, 0, "", json)
                      virtualMessage.appId = appId
                      virtualMessage.sdk = msg.sdk
                      virtualMessage.zgid = msg.zgid
                      virtualMessage.zgeid = msg.zgeid
                      val map: java.util.Map[String, AnyRef] = JsonUtil.mapFromJson(virtualMessage.rawData)
                      if (map == null) {
                        virtualMessage.result = -1;
                        virtualMessage.error = "msg not json"
                        virtualMessage.errorCode += ErrorMessageEnum.JSON_FORMAT_ERROR.getErrorCode
                        //json解析异常
                        val errorInfo = ErrorMessageEnum.JSON_FORMAT_ERROR.getErrorMessage
                        virtualMessage.errorDescribe += errorInfo
                        IdLogger.errorFormat(errorInfo, virtualMessage.rawData)
                      } else {
                        virtualMessage.data = map
                      }
                      list.add(virtualMessage)
                    }
                  }
                )
              }
            }
          }
        }

      }
    })
    list.forEach(virtualMsg => {
      msgs += virtualMsg
    })
  }

  def getCompareResult(dt: String, pr: java.util.Map[String, java.lang.Object], matchJsonArr: JSONArray, msg: ZGMessage): Boolean = {
    matchJsonArr.forEach(
      matchJsonObj => {
        val matchJson = matchJsonObj.asInstanceOf[JSONObject]
        val label = matchJson.getString("label") // 如 "_广告分析链接ID":183,
        var eventAttValue = String.valueOf(pr.get("_" + label))
        if ("abp" == dt) {
          //获取内置事件属性值
          eventAttValue = String.valueOf(pr.get("$" + label))
        }
        if ("业务".equals(label)) {
          eventAttValue = msg.business
        }
        val boolean: lang.Boolean = OperatorUtil.compareValue(eventAttValue, matchJson, label)
        if (!boolean) {
          return false
        }
      })
    return true
  }


}
