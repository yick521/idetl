package com.zhugeio.etl.id.service

import com.zhugeio.etl.commons.{Dims, ErrorMessageEnum}
import com.zhugeio.etl.id.ZGMessage
import com.zhugeio.etl.id.log.IdLogger

import scala.collection.mutable.ListBuffer

/**
  * Created by ziwudeng on 9/13/16.
  */
object AppService {


  def parseAppId(msgs: ListBuffer[ZGMessage]): Unit = {
    msgs.foreach(msg => {
      if (msg.result != -1) {
        val plat: Integer = Dims.sdk(String.valueOf(msg.data.get("pl")))
        msg.data.put("plat", plat)
        msg.sdk = plat
        val appKey: String = String.valueOf(msg.data.get("ak"))
        val appIdOption = FrontService.getAppId(appKey);
        if (appIdOption.isEmpty) {
          msg.result = -1
          msg.errorCode += ErrorMessageEnum.AK_NONE.getErrorCode
          //ak在应用管理中不存在或已被删除
          val errorInfo = ErrorMessageEnum.AK_NONE.getErrorMessage
          msg.errorDescribe += errorInfo
          msg.errData = msg.data
          IdLogger.errorFormat(errorInfo, msg.rawData)
        } else {
          msg.appId = appIdOption.get
          var business = ""
          if (msg.data.containsKey("business") && null != msg.data.get("business")) {
            business = msg.data.get("business").toString
          }
          msg.business = business
          msg.data.put("app_id", appIdOption.get)
          FrontService.handleAppPlat(appIdOption.get, plat)
          FrontService.handleAppUpload(appIdOption.get, plat)
        }
      }
    })
  }
}
