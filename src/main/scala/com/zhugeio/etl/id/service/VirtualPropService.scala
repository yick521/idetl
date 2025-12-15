package com.zhugeio.etl.id.service

import com.alibaba.fastjson.serializer.SerializerFeature
import com.alibaba.fastjson.{JSON, JSONObject}
import com.fasterxml.jackson.databind.ObjectMapper
import com.zhugeio.etl.commons.ErrorMessageEnum
import com.zhugeio.etl.id.ZGMessage
import com.zhugeio.etl.id.cache.FrontCache
import com.zhugeio.etl.id.log.IdLogger
import com.zhugeio.etl.virtualAttribute.VirtualAttributeExpressionEvaluator

import java.util
import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer
import scala.language.postfixOps


object VirtualPropService {

  def handleVirtualProps(msgs: ListBuffer[ZGMessage]): Unit = {
    msgs.foreach(msg =>
    {
      if (msg.result != -1) {
        val appId = msg.appId
        if (FrontCache.virtualPropAppIdsSet.contains(appId.toString)) {
          val dataItemIterator = msg.data.get("data").asInstanceOf[java.util.List[java.util.Map[String, java.lang.Object]]].iterator()
          while (dataItemIterator.hasNext) {
            val dataItem = dataItemIterator.next()
            val dt = String.valueOf(dataItem.get("dt"))
            val prObj = dataItem.get("pr")
            val pr: JSONObject = prObj match {
              case json: JSONObject => json
              case map: java.util.Map[_, _] =>
                val jsonString = JSON.toJSONString(map, SerializerFeature.WRITE_MAP_NULL_FEATURES)
                JSON.parseObject(jsonString)
              case _ => new JSONObject()
            }
            if ("evt" == dt || "abp" == dt) {
              val eventName = pr.get("$eid").asInstanceOf[String]
              val key = s"${appId}_eP_$eventName"
              if (FrontCache.virtualEventPropMap.contains(key)) {
                val virtualPropJsonStrArr: util.ArrayList[String] = FrontCache.virtualEventPropMap(key)
                virtualPropJsonStrArr.forEach(virtualPropJsonStr => {
                  val virtualPropJsonObj = JSON.parseObject(virtualPropJsonStr)
                  val virtualPropName = virtualPropJsonObj.getString("name")
                  val virtualPropRule = virtualPropJsonObj.getString("define")
                  // 将虚拟属性添加进 pr
                  handleVirtualProp(virtualPropName, virtualPropRule, pr)
                  dataItem.put("pr", pr)
                })
              }
            }else if ("usr" == dt) {
              val key = appId.toString
              if (FrontCache.virtualUserPropMap.contains(key)) {
                val virtualPropJsonStrArr: util.ArrayList[String] = FrontCache.virtualUserPropMap(key)
                virtualPropJsonStrArr.forEach(virtualPropJsonStr => {
                  val virtualPropJsonObj: JSONObject = JSON.parseObject(virtualPropJsonStr)
                  val virtualPropName = virtualPropJsonObj.getString("name")
                  val virtualPropRule = virtualPropJsonObj.getString("define")
                  val tableFields = virtualPropJsonObj.getString("tableFields")
                  // 判断当前用户数据是否包含所有虚拟属性字段
                  val is_all = isAllProp(tableFields, pr)
                  // 将用户虚拟属性添加进 pr
                  if(is_all){
                    handleVirtualProp(virtualPropName, virtualPropRule, pr)
                    dataItem.put("pr", pr)
                  }
                })
              }
            }
          }
        }
      }
    })
  }

  /**
   * 判断当前数据中是否包含所有虚拟属性字段
   * @param tableFields  虚拟属性表名字段名
   * @param pr  数据
   * @return true: 包含所有虚拟属性字段 false: 不包含所有虚拟属性字段
   */
  private def isAllProp(tableFields: String, pr: JSONObject): Boolean = {
    if (tableFields == null || tableFields.isEmpty) {
      return false
    }

    val tableFieldArr = tableFields.split(",").map(_.trim).filter(_.nonEmpty)
    val requiredFields = tableFieldArr.map { field =>
      val parts = field.split("\\.")
      "_" + parts.last
    }.toSet
    val prKeys = pr.keySet().asScala.toSet
    requiredFields.forall(key => prKeys.contains(key))
  }

  private def handleVirtualProp(virtualPropName: String, virtualPropRule: String, pr: JSONObject) = {
    try {
      val evaluator = new VirtualAttributeExpressionEvaluator
      val objectMapper = new ObjectMapper()
      val jsonNode = objectMapper.readTree(pr.toJSONString)
      val value = evaluator.evaluate(virtualPropRule, jsonNode)
      //将布尔值转换成1或0，并处理数值格式问题
      val convertedValue = value match {
        case booleanValue: java.lang.Boolean => if (booleanValue) 1 else 0
        case doubleValue: java.lang.Double =>
          if (doubleValue.isInfinite || doubleValue.isNaN) {
            null
          } else if (doubleValue == doubleValue.toLong) {
            // 如果是整数，转换为Long避免科学计数法
            java.math.BigDecimal.valueOf(doubleValue.toLong)
          } else {
            // 保留小数，但避免科学计数法
            java.math.BigDecimal.valueOf(doubleValue)
          }
        case floatValue: java.lang.Float =>
          val doubleVal = floatValue.toDouble
          if (floatValue.isInfinite || floatValue.isNaN) {
            null
          } else if (doubleVal == doubleVal.toLong) {
            // 如果是整数，转换为Long避免科学计数法
            java.math.BigDecimal.valueOf(doubleVal.toLong)
          } else {
            // 保留小数，但避免科学计数法
            java.math.BigDecimal.valueOf(doubleVal)
          }
        case bigDecimalValue: java.math.BigDecimal =>
          if (bigDecimalValue.stripTrailingZeros().scale() <= 0) {
            // 去除尾随零后，如果小数位数 <= 0，说明是整数
            if (bigDecimalValue.compareTo(new java.math.BigDecimal((java.lang.Long.MAX_VALUE))) <= 0 &&
              bigDecimalValue.compareTo(new java.math.BigDecimal(java.lang.Long.MIN_VALUE)) >= 0) {
              bigDecimalValue.longValue()
            } else {
              bigDecimalValue.toBigInteger
            }
          } else {
            bigDecimalValue
          }
        case _ => value
      }
      //      println("virtualPropName is: " + virtualPropName + " virtualValue is : " + convertedValue)
      pr.put("_" + virtualPropName, convertedValue)
    } catch {
      case e: Exception =>
        val errorInfo = ErrorMessageEnum.VIRTUAL_ATTR_FIELD.getErrorMessage
        IdLogger.errorFormat(errorInfo,s"${e.getMessage}")
      // 可以选择跳过这个虚拟属性或设置默认值
    }
  }


}