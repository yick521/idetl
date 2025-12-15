package com.zhugeio.etl.id.service

import java.util.function.Consumer

import com.zhugeio.etl.commons.ErrorMessageEnum
import com.zhugeio.tool.commons.JsonUtil
import com.zhugeio.etl.id.ZGMessage
import com.zhugeio.etl.id.log.IdLogger

import scala.collection.mutable.ListBuffer
import scala.collection.JavaConverters._;

/**
  * Created by ziwudeng on 9/13/16.
  */
object JsonService {

  /**
    * parseJson
    * parse string to json ,put in ZGMessage
    */

  def parseJson(msgs: ListBuffer[ZGMessage]): Unit = {
    msgs.foreach(msg => {
      val map: java.util.Map[String, AnyRef] = JsonUtil.mapFromJson(msg.rawData)
      val errMap: java.util.Map[String, AnyRef] = JsonUtil.mapFromJson(msg.rawData)
      if (map == null) {
        msg.result = -1;
        msg.error = "msg not json"
        msg.errorCode = ErrorMessageEnum.JSON_FORMAT_ERROR.getErrorCode
        //json解析异常
        msg.errorDescribe = ErrorMessageEnum.JSON_FORMAT_ERROR.getErrorMessage
        msg.errData = msg.data
        IdLogger.errorFormat(ErrorMessageEnum.JSON_FORMAT_ERROR.getErrorMessage, msg.rawData)
      } else {
        var owner = map.get("owner").asInstanceOf[String]
        owner match {
          case "zg_adp" =>
            owner = "zg_adp"
          case "zg_mkt" =>
            owner = "zg_mkt"
          case "zg_cdp" =>
            owner = "zg_cdp"
          case _ =>
            owner = "zg"
        }
        map.put("owner", owner)
        msg.data = map
        errMap.remove("data")
        msg.errData = errMap
      }
    })
  }


  def saveJson(msgs: ListBuffer[ZGMessage]): Unit = {
    msgs.foreach(msg => {
      if (msg.result != -1) {
        msg.json = JsonUtil.toJson(msg.data)
      }
    })
  }
}
