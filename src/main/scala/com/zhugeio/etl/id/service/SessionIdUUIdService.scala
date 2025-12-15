package com.zhugeio.etl.id.service

import com.zhugeio.etl.id.ZGMessage

import scala.collection.mutable.ListBuffer

/**
  * Created by ziwudeng on 9/19/16.
  */
object SessionIdUUIdService {

  def addSessionIds(msgs: ListBuffer[ZGMessage]): Unit = {

    msgs.foreach(msg => {

      if (msg.result != -1) {
        val iterator = msg.data.get("data").asInstanceOf[java.util.List[java.util.Map[String, Object]]].iterator()
        while (iterator.hasNext) {
          val item = iterator.next()
          val pr = item.get("pr").asInstanceOf[java.util.Map[String, java.lang.Object]]
          if (pr.containsKey("$sid")) {
            val sid = new java.math.BigDecimal(String.valueOf(pr.get("$sid"))).longValue()
            pr.put("$zg_sid", new java.lang.Long(sid))
          } else {
            pr.put("$zg_sid", new java.lang.Long(-1));
          }
          val dt = item.get("dt")
          if ("evt" == dt || "ss" == dt || "se" == dt || "mkt" == dt || "abp" == dt) {
            pr.put("$uuid", generateUUId())
          }
        }
      }
    })
  }

  def generateUUId(): String = {
    java.util.UUID.randomUUID().toString.replaceAll("-", "");
  }

  def generateSessionId(sid: String, zgDeviceId: Integer): java.lang.Long = {
    (sid.toLong / 1000 + "%09d".format(zgDeviceId)).toLong
  }

}
