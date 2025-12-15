package com.zhugeio.etl.id.service

import java.text.SimpleDateFormat
import java.util.Date
import com.zhugeio.etl.commons.ErrorMessageEnum
import com.zhugeio.etl.id.cache.FrontCache
import com.zhugeio.etl.id.cache.FrontCache.virtualEventAttrMap
import com.zhugeio.etl.id.commons.TypeJudger
import com.zhugeio.etl.id.db.Datasource
import com.zhugeio.etl.id.log.IdLogger
import com.zhugeio.etl.id.utils.ValiDateStrUtil
import com.zhugeio.etl.id.{Config, ZGMessage}
import com.zhugeio.tool.sdk.config.SdkConfig
import org.apache.commons.lang3.StringUtils
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
  * Created by ziwudeng on 9/14/16.
  */
object EventService {

  //  val logger = Logger.getLogger(this.getClass)
  val logger: Logger = LoggerFactory.getLogger(this.getClass)
  val maxEventLength: Int = Config.getInt(Config.MAX_EVENT_LENGTH);
  val maxAttrLength: Int = Config.getInt(Config.MAX_ATTR_LENGTH);
  SdkConfig.init(Datasource.front, Config.getInt(Config.SDK_CONFIG_FLUSH))

  def handleEventPropIds(msgs: ListBuffer[ZGMessage]): Unit = {

    val (events, eventAttrs) = collectEvents(msgs);
    val eventIdMap = handleEvents(events) //事件个数超限制
    val eventAttrIdMap = handleEventAttrs(eventIdMap, eventAttrs);
    for (msg <- msgs) {
      if (msg.result != -1) {
        val appId = msg.appId
        var defaultOwner = msg.data.get("owner").asInstanceOf[String]
        val dataItemIterator = msg.data.get("data").asInstanceOf[java.util.List[java.util.Map[String, java.lang.Object]]].iterator()
        while (dataItemIterator.hasNext) {
          val dataItem = dataItemIterator.next()
          var dt = String.valueOf(dataItem.get("dt"))
          if ("evt" == dt || "mkt" == dt || "abp" == dt || "vtl" == dt) {
            val pr = dataItem.get("pr").asInstanceOf[java.util.Map[String, java.lang.Object]]
            var oldOwner = defaultOwner
            var oldDt = dt
            if ("vtl" == dt) {
              oldOwner = String.valueOf(pr.get("$event_owner"))
              oldDt = String.valueOf(pr.get("$event_dt"))
            }
            var owner = getOwner(dt, defaultOwner)
            val propPrefix = getAttrPrefix(oldDt);
            //TODO 内置事件形成的虚拟事件，只有指定的属性名才能入库 isAttrNameValid
            val eventName = String.valueOf(pr.get("$eid"))
            val eventIdOption = eventIdMap.get(new Tuple3[Integer, String, String](appId, owner, eventName))
            if (eventIdOption.isEmpty || eventIdOption.get.isEmpty) {
              var errDataItem: java.util.List[java.util.Map[String, java.lang.Object]] = new java.util.ArrayList[java.util.Map[String, java.lang.Object]]()
              if(null == msg.errData){
                msg.errData = new java.util.HashMap[String, java.lang.Object]()
              }
              if (msg.errData.containsKey("data")) {
                errDataItem = msg.errData.get("data").asInstanceOf[java.util.ArrayList[java.util.Map[String, java.lang.Object]]]
              }
              errDataItem.add(dataItem)
              dataItem.put("errorCode", ErrorMessageEnum.EVENT_NUMBER_LIMIT.getErrorCode.toString)
              dataItem.put("errorDescribe", ErrorMessageEnum.EVENT_NUMBER_LIMIT.getErrorMessage)
              msg.errData.put("data", errDataItem)
              dataItemIterator.remove()
            } else {
              val eventId = eventIdOption.get.get
              pr.put("$zg_eid", eventId)
              msg.zgeid = eventId
              FrontCache.eventlastInsertTime += (eventId.toInt -> getNowDateString())
//              logger.info("addEventLastInsertTime1,eventlastInsertTime:"+FrontCache.eventlastInsertTime)
              //get propType
              val customPropTypeMap = scala.collection.mutable.HashMap[String, String]()
              val propIterator = pr.entrySet().iterator()
              while (propIterator.hasNext) {
                val propItem = propIterator.next()
                if (propItem.getKey.startsWith(propPrefix) && isAttrNameValid(oldDt, propItem.getKey.substring(1))) {
                  val objType = TypeJudger.getObjectType(propItem.getValue)
                  customPropTypeMap.put(propItem.getKey.substring(1), objType)
                } else if (isDistinct(propPrefix) && propItem.getKey.startsWith("_") && !isAttrNameValid(oldDt, propItem.getKey.substring(1))) {
                  val objType = TypeJudger.getObjectType(propItem.getValue)
                  customPropTypeMap.put(propItem.getKey.substring(1), objType)
                }
              }
              var isErr = true
              customPropTypeMap.foreach(m => {
                val propName = m._1
                val propType = m._2
                val propId = eventAttrIdMap.get(Tuple3(appId, eventId, propName))
                if (propId.isEmpty || propId.get.isEmpty) {
                  //特殊处理, 埋点消息不会被过滤掉，事件属性会被过滤掉
                  if (isErr) {
                    var errDataItem: java.util.List[java.util.Map[String, java.lang.Object]] = new java.util.ArrayList[java.util.Map[String, java.lang.Object]]()
                    if(null == msg.errData){
                      msg.errData = new java.util.HashMap[String, java.lang.Object]()
                    }
                    if (msg.errData.containsKey("data")) {
                      errDataItem = msg.errData.get("data").asInstanceOf[java.util.ArrayList[java.util.Map[String, java.lang.Object]]]
                    }
                    errDataItem.add(dataItem)
                    dataItem.put("errorCode", ErrorMessageEnum.EVENT_ATTR_ID_ERROR.getErrorCode.toString)
                    dataItem.put("errorDescribe", ErrorMessageEnum.EVENT_ATTR_ID_ERROR.getErrorMessage)
                    msg.errData = new java.util.HashMap[String, java.lang.Object]()
                    msg.errData.put("data", errDataItem)
                    IdLogger.errorFormat(ErrorMessageEnum.EVENT_ATTR_ID_ERROR.getErrorMessage, msg.rawData)
                    isErr = false
                  }
                  pr.remove(propName)
                } else {
                  if (!isDistinct(propPrefix)) {
                    pr.put("$zg_epid#" + propPrefix + propName, propId.get.get)
                    pr.put("$zg_eptp#" + propPrefix + propName, propType)
                  } else {
                    if (isDistinct(propPrefix) && !isAttrNameValid(dt, propName)) {
                      pr.put("$zg_epid#_" + propName, propId.get.get)
                      pr.put("$zg_eptp#_" + propName, propType)
                    } else {
                      pr.put("$zg_epid#" + propPrefix + propName, propId.get.get)
                      pr.put("$zg_eptp#" + propPrefix + propName, propType)
                    }
                  }
                }
              })
            }
          } else if ("ss".equals(dataItem.get("dt").asInstanceOf[String])) {
            dataItem.get("pr").asInstanceOf[java.util.Map[String, java.lang.Object]].put("$zg_eid", new Integer(-1))
          } else if ("se".equals(dataItem.get("dt").asInstanceOf[String])) {
            dataItem.get("pr").asInstanceOf[java.util.Map[String, java.lang.Object]].put("$zg_eid", new Integer(-2))
          }
        }
      }
    }
  }

  def collectEvents(msgs: ListBuffer[ZGMessage]): Tuple2[mutable.Set[Tuple5[Integer, String, String, String, Integer]], mutable.Map[Tuple6[Integer, String, String, String, String, Integer], String]] = {
    val events = java.util.Collections.newSetFromMap(
      new java.util.concurrent.ConcurrentHashMap[Tuple5[Integer, String, String, String, Integer], java.lang.Boolean]).asScala
    val eventAttrs = new java.util.concurrent.ConcurrentHashMap[Tuple6[Integer, String, String, String, String, Integer], String]().asScala
    for(msg <- msgs){
      if (msg.result != -1) {
        val appId = msg.appId
        var defaultOwner = msg.data.get("owner").asInstanceOf[String]
        val sdk = msg.sdk
        val dataItemIterator = msg.data.get("data").asInstanceOf[java.util.List[java.util.Map[String, java.lang.Object]]].iterator()
        while (dataItemIterator.hasNext) {
          val dataItem = dataItemIterator.next()
          var dt = String.valueOf(dataItem.get("dt"))
          if ("evt" == dt || "mkt" == dt || "abp" == dt || "vtl" == dt) {
            val pr = dataItem.get("pr").asInstanceOf[java.util.Map[String, java.lang.Object]]
            var oldOwner = defaultOwner
            var oldDt = dt
            if ("vtl" == dt) {
              oldOwner = String.valueOf(pr.get("$event_owner"))
              oldDt = String.valueOf(pr.get("$event_dt"))
            }
            val owner = getOwner(dt, defaultOwner)
            var propPrefix = getAttrPrefix(oldDt);
            val eventName = String.valueOf(pr.get("$eid")).trim
            pr.put("$eid", eventName)
            val flagTupe: (Boolean, ErrorMessageEnum) = isEventNameInvalid(dt, eventName)
            if (flagTupe._1) {
              var errDataItem: java.util.List[java.util.Map[String, java.lang.Object]] = new java.util.ArrayList[java.util.Map[String, java.lang.Object]]()
              if(null == msg.errData){
                msg.errData = new java.util.HashMap[String, java.lang.Object]()
              }
              if (msg.errData.containsKey("data")) {
                errDataItem = msg.errData.get("data").asInstanceOf[java.util.ArrayList[java.util.Map[String, java.lang.Object]]]
              }
              errDataItem.add(dataItem)
              dataItem.put("errorCode", flagTupe._2.getErrorCode + "")
              dataItem.put("errorDescribe", flagTupe._2.getErrorMessage)
              msg.errData.put("data", errDataItem)
              dataItemIterator.remove()
            } else {
              var aliasName = getAliasEventName(dt, eventName)
              if ("vtl" == dt) {
                //虚拟事件的埋点事件必须先入库才可被列为某个虚拟事件下
                aliasName = pr.get("$virtual_alias").asInstanceOf[String]
              }
              events.add(new Tuple5[Integer, String, String, String, Integer](appId, owner, eventName, aliasName, sdk))
              //get propType
              val customPropTypeMap = scala.collection.mutable.HashMap[String, String]()
              val propIterator = pr.entrySet().iterator()
              while (propIterator.hasNext) {
                val propItem = propIterator.next()
                if (propItem.getKey.startsWith(propPrefix) && isAttrNameValid(oldDt, propItem.getKey.substring(1))) {
                  val objType = TypeJudger.getObjectType(propItem.getValue)
                  customPropTypeMap.put(propItem.getKey.substring(1), objType)
                } else if (isDistinct(propPrefix) && propItem.getKey.startsWith("_") && !isAttrNameValid(oldDt, propItem.getKey.substring(1))) {
                  val objType = TypeJudger.getObjectType(propItem.getValue)
                  customPropTypeMap.put(propItem.getKey.substring(1), objType)
                }
              }
              //trim and filter propName
              customPropTypeMap.foreach(m => {
                val propName = m._1.trim
                if (propName.length < m._1.length) {
                  val value = pr.get(propPrefix + m._1)
                  if (value != null) {
                    pr.remove(propPrefix + m._1)
                    pr.put(propPrefix + propName, value);
                  } else if (isDistinct(propPrefix)) {
                    pr.remove("_" + m._1)
                    pr.put("_" + propName, value);
                  }
                }
                val propType = m._2
                if ("null".equals(propType)) {
                  val v = pr.remove(propPrefix + propName)
                  if (v == null) {
                    pr.remove("_" + propName)
                  }
                } else if (propName.length > maxAttrLength) {
                  val v = pr.remove(propPrefix + propName)
                  if (v == null) {
                    pr.remove("_" + propName)
                  }
                } else {
                  var flag = true
                  var attrAliasName = ""
                  if ("vtl" == dt) {
                    //虚拟事件的埋点事件必须先入库才可被列为某个虚拟事件下
                    val oldEventOwner = pr.get("$event_owner").asInstanceOf[String]
                    val oldEventName = pr.get("$event_name").asInstanceOf[String]
                    val attrKey = s"${appId}_${eventName}_${oldEventOwner}_${oldEventName}"
                    val option: Option[Set[String]] = virtualEventAttrMap.get(attrKey)
                    if (option.isDefined) {
                      val attrSet = option.get
                      val key = s"${appId}_${oldEventOwner}_${oldEventName}_${propName}"
                      if (FrontCache.eventAttrAliasMap.contains(key)) {
                        attrAliasName = FrontCache.eventAttrAliasMap(key)
                      }
                      if (attrSet.nonEmpty && !attrSet.contains(propName)) {
                        //有属性限制
                        flag = false
                      }
                    }
                  }
                  if(StringUtils.isBlank(propName) || !ValiDateStrUtil.isValidDateStr(propName)){
                    var errDataItem: java.util.List[java.util.Map[String, java.lang.Object]] = new java.util.ArrayList[java.util.Map[String, java.lang.Object]]()
                    if(null == msg.errData){
                      msg.errData = new java.util.HashMap[String, java.lang.Object]()
                    }
                    if (msg.errData.containsKey("data")) {
                      errDataItem = msg.errData.get("data").asInstanceOf[java.util.ArrayList[java.util.Map[String, java.lang.Object]]]
                    }
                    errDataItem.add(dataItem)
                    dataItem.put("errorCode", ErrorMessageEnum.EVENT_ATTR_INVALID.getErrorCode + "")
                    dataItem.put("errorDescribe", ErrorMessageEnum.EVENT_ATTR_INVALID.getErrorMessage)
                    msg.errData.put("data", errDataItem)
                  }else if (flag) {
                    eventAttrs.putIfAbsent(new Tuple6[Integer, String, String, String, String, Integer](appId, owner, eventName, propName, attrAliasName, sdk), propType);
                  }
                }
              })
            }
          }
        }
      }
    }
    Tuple2(events, eventAttrs)
  }


  def handleEvents(events: mutable.Set[Tuple5[Integer, String, String, String, Integer]]): mutable.HashMap[Tuple3[Integer, String, String], Option[Integer]] = {
    val eventIdMap = new mutable.HashMap[Tuple3[Integer, String, String], Option[Integer]]();
    events.foreach(a => {
      val (appId, owner, eventName, aliasName, sdk) = a;
      val eventId = FrontService.getEventId(appId, owner, eventName, aliasName)
      eventIdMap.put(new Tuple3[Integer, String, String](a._1, a._2, a._3), eventId);
      if (eventId.isDefined) {
        FrontService.handleEventPlatform(appId, eventId.get, sdk);
      }
    })
    eventIdMap
  }

  def handleEventAttrs(eventIdMap: mutable.Map[Tuple3[Integer, String, String], Option[Integer]], eventAttrs: mutable.Map[Tuple6[Integer, String, String, String, String, Integer], String]): mutable.HashMap[Tuple3[Integer, Integer, String], Option[Integer]] = {
    val eventAttrIdMap = new mutable.HashMap[Tuple3[Integer, Integer, String], Option[Integer]]();
    eventAttrs.foreach(a => {
      val (appId, owner, eventName, propName, attrAliasName, sdk) = a._1;
      val propType = a._2
      val eventId = eventIdMap.get(new Tuple3[Integer, String, String](appId, owner, eventName));
      if (eventId.isDefined && eventId.get.isDefined) {
        val propId = FrontService.getEventAttrId(appId, eventId.get.get, owner, propName, attrAliasName, TypeJudger.getIntType(propType))
        eventAttrIdMap.put(new Tuple3[Integer, Integer, String](appId, eventId.get.get, propName), propId)
        if (propId.isDefined) {
          FrontService.handleEventAttrPlat(appId, eventId.get.get, propId.get, sdk)
        }
      }
    })
    eventAttrIdMap
  }

  //  def isEventNameInvalid(dt: String, eventName: String): Boolean = {
  //    dt match {
  //      case "evt" => eventName.length > maxEventLength
  //      case "vtl" => eventName.length > maxEventLength
  //      case "mkt" => eventName.length > maxEventLength || !SdkConfig.isMktEvents(eventName)
  //      case "abp" => eventName.length > maxEventLength || !SdkConfig.isAbpEvents(eventName)
  //      case _ => true
  //    }
  //  }

  def isEventNameInvalid(dt: String, eventName: String): (Boolean, ErrorMessageEnum) = {
    if ("".equals(eventName)) {
      return (true, ErrorMessageEnum.EID_NONE)
    }else if (eventName.length > maxEventLength) {
      return (true, ErrorMessageEnum.EVENT_NAME_LENGTH_LIMIT)
    }else if(!ValiDateStrUtil.isValidDateStr(eventName)){
      return (true, ErrorMessageEnum.EVENT_NAME_INVALID)
    }else {
      dt match {
        case "evt" =>
          (false, ErrorMessageEnum.NONE_ERROR)
        case "vtl" =>
          (false, ErrorMessageEnum.NONE_ERROR)
        case "mkt" => {
          if (!SdkConfig.isMktEvents(eventName)) {
            (true, ErrorMessageEnum.MKT_EVENT_NOT_SPECIFIED)
          } else {
            (false, ErrorMessageEnum.NONE_ERROR)
          }
        }
        case "abp" => {
          if (!SdkConfig.isAbpEvents(eventName)) {
            (true, ErrorMessageEnum.BUILTIN_EVENT_NOT_SPECIFIED)
          } else {
            (false, ErrorMessageEnum.NONE_ERROR)
          }
        }
        case _ => (true, ErrorMessageEnum.EVENT_TYPE_ERROR)
      }
    }
  }

  def getAttrPrefix(dt: String): String = {
    dt match {
      case "evt" => "_"
      case "mkt" => "$"
      case "abp" => "$"
      case _ => "_"
    }
  }

  def isAttrNameValid(dt: String, attrName: String): Boolean = {
    dt match {
      case "evt" => true
      case "mkt" => SdkConfig.isMktAttrNameValid(attrName)
      case "abp" => SdkConfig.isAbpAttrNameValid(attrName)
      case _ => true
    }
  }

  def isDistinct(propPrefix: String): Boolean = {
    propPrefix match {
      case "$" => true
      case _ => false
    }
  }

  def getAliasEventName(dt: String, eventName: String): String = {
    dt match {
      case "evt" => null
      case "mkt" => null
      case "abp" => SdkConfig.getAbpAliasEventName(eventName)
      case _ => null
    }
  }

  def getOwner(dt: String, defaulOwner: String): String = {
    dt match {
      case "evt" => defaulOwner
      case "mkt" => "zg_mkt"
      case "abp" => "zg_abp"
      case _ => defaulOwner
    }
  }

  def getNowDateString(): String = {
    val formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    formatter.format(new Date())
  }


}
