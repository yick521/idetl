package com.zhugeio.etl.id

import scala.collection.mutable.ListBuffer

/**
  * Created by ziwudeng on 9/13/16.
  */

class ZGMessage(var topic: String, val partition: Integer, var offset: Long, var key: String, var rawData: String) {


  var result: Integer = 0 // 0 正常 -1有异常

  var appId: Integer = 0

  var business: String = ""

  var sdk: Integer = 0

  var data: java.util.Map[String, java.lang.Object] = null

  var errData: java.util.Map[String, java.lang.Object] = null

  var json: String = ""

  var error: String = ""

  var zgid: java.lang.Long = 0L

  var zgeid: Integer = 0

  var errorCode = 0

  var errorDescribe = ""

  override def toString = s"ZGMessage(result=$result, appId=$appId, sdk=$sdk, json=$json, rawData=$rawData, error=$error, topic=$topic, partition=$partition, offset=$offset, key=$key)"

}