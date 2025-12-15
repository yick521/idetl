package com.zhugeio.etl.id.service

import com.zhugeio.etl.id.{Config, ZGMessage}
import com.zhugeio.etl.id.commons.TypeJudger

import scala.collection.mutable.ListBuffer

/**
  * Created by ziwudeng on 9/14/16.
  */
object DevicePropService {

  val maxDevAttrLength: Int = Config.getInt(Config.MAX_DEV_PROP_LENGTH)

  def handleDevicePropIds(msgs: ListBuffer[ZGMessage]): Unit = {

    msgs.foreach(msg => {
      if (msg.result != -1) {
        val appId = msg.appId
        val owner = msg.data.get("owner").asInstanceOf[String]
        val sdk = msg.sdk
        val dataItemIterator = msg.data.get("data").asInstanceOf[java.util.List[java.util.Map[String, java.lang.Object]]].iterator()

        while (dataItemIterator.hasNext) {

          val dataItem = dataItemIterator.next()

          if ("pl".equals(dataItem.get("dt").asInstanceOf[String])) {

            val pr = dataItem.get("pr").asInstanceOf[java.util.Map[String, java.lang.Object]]
            val customPropMap = scala.collection.mutable.HashMap[String, String]()
            val propIterator = pr.entrySet().iterator()

            while (propIterator.hasNext) {
              val propItem = propIterator.next()
              if (propItem.getKey.startsWith("_")) {
                val objType = TypeJudger.getObjectType(propItem.getValue)
                customPropMap.put(propItem.getKey.substring(1), objType)
              }
            }
            customPropMap.foreach(m => {
              //trim propName
              val propName = m._1.trim
              if (propName.length < m._1.length) {
                val value = pr.get("_" + m._1)
                pr.remove(m._1)
                pr.put(propName, value);
              }
              val propType = m._2
              if ("null".equals(propType)) {
                pr.remove(propName)
              } else if (propName.length > maxDevAttrLength) {
                pr.remove(propName)
              } else {
                val propIdOption = FrontService.getDevicePropId(appId, owner, propName)
                FrontService.handleDevicePropPlat(appId, propIdOption.get, sdk)
                pr.put("$zg_dpid#_" + propName, propIdOption.get)
                pr.put("$zg_dptp#_" + propName, propType)
              }
            })
          }
        }

      }
    })
  }


}
