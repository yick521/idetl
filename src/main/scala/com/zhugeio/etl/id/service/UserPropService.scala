package com.zhugeio.etl.id.service

import com.zhugeio.etl.id.cache.FrontCache.appIdPropIdOriginalMap
import com.zhugeio.etl.id.{Config, ZGMessage}
import com.zhugeio.etl.id.commons.TypeJudger

import scala.collection.mutable.ListBuffer

/**
  * Created by ziwudeng on 9/14/16.
  */
object UserPropService {

  val maxPropLength = Config.getInt(Config.MAX_PROP_LENGTH);

  def handleUserPropIds(msgs: ListBuffer[ZGMessage]): Unit = {

    msgs.foreach(msg => {
      if (msg.result != -1) {
        val appId = msg.appId
        val owner = String.valueOf(msg.data.get("owner"))
        val dataItemIterator = msg.data.get("data").asInstanceOf[java.util.List[java.util.Map[String, java.lang.Object]]].iterator()

        while (dataItemIterator.hasNext) {

          val dataItem = dataItemIterator.next()

          if ("usr".equals(dataItem.get("dt").asInstanceOf[String])) {

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
              var propName = m._1
              val propType = m._2
              if ("null".equals(propType)) {
                pr.remove(propName)
              } else if (propName.length > maxPropLength) {
                pr.remove(propName)
              } else {
                val propIdOption = FrontService.getUserPropId(appId, owner, propName, TypeJudger.getIntType(propType))
                if (propIdOption.isDefined) {
                  if (appIdPropIdOriginalMap.contains(s"${appId}_${owner}_${propIdOption.get}")) {
                    if (propName != appIdPropIdOriginalMap(s"${appId}_${owner}_${propIdOption.get}")) {
                      val va = pr.get("_" + propName)
                      pr.remove("_" + propName)
                      propName = appIdPropIdOriginalMap(s"${appId}_${owner}_${propIdOption.get}")
                      pr.put("_" + propName, va)
                    }
                  }
                  pr.put("$zg_upid#_" + propName, propIdOption.get)
                  pr.put("$zg_uptp#_" + propName, propType)
                } else {
                  pr.remove(propName)
                }
              }
            })
          }
        }

      }
    })
  }


}
