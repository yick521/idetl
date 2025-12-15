package com.zhugeio.etl.id.service

import com.alibaba.fastjson.JSONObject

import java.util
import java.util.{Date, HashMap, Random}
import java.util.concurrent.Future
import java.io.InputStream
import java.text.SimpleDateFormat
import com.zhugeio.etl.commons.{Dims, ErrorMessageEnum}
import com.zhugeio.etl.id.adtoufang.util.ToolUtil
import com.zhugeio.tool.commons.JsonUtil
import com.zhugeio.etl.id.{Config, ZGMessage}
import org.apache.commons.io.IOUtils
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord, RecordMetadata}

import scala.collection.mutable.ListBuffer
import scala.collection.JavaConverters._
import scala.collection.mutable;

/**
  * Created by ziwudeng on 9/18/16.
  */
object DistributeService {

  val brokers = Config.getProp(Config.KAFKA_BROKERS)
  val props = new HashMap[String, Object]()
  props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers)
  props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
    "org.apache.kafka.common.serialization.StringSerializer")
  props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
    "org.apache.kafka.common.serialization.StringSerializer")
  props.put(ProducerConfig.ACKS_CONFIG, "0")
  props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy")
  props.put(ProducerConfig.LINGER_MS_CONFIG, "100")
  if (Config.getProp(Config.KAFKA_BUFFER_MEMORY) != null) {
    props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, Config.getProp(Config.KAFKA_BUFFER_MEMORY).trim)
  }

  val producerMap = new mutable.HashMap[Integer, KafkaProducer[String, String]]()

  var producerSingles = 1;
  if (Config.getProp(Config.KAFKA_PRODUCER_NUMBER) != null) {
    producerSingles = Config.getInt(Config.KAFKA_PRODUCER_NUMBER);
  }

  for (i <- 0 to producerSingles - 1) {
    producerMap.put(i, new KafkaProducer[String, String](props))
  }

  val random = new Random()

  val typeTopicMap = getTypeTopicMap

  val error_count_key = "error_count"

  val error_pro_count_key = "error_pro_count"

  val error_Log_Type = "error-log"

  val data_count_type = "data-count"


  def getTypeTopicMap: scala.collection.mutable.Map[String, String] = {
    var in: InputStream = null
    try {
      in = this.getClass.getClassLoader.getResourceAsStream(Config.DTYPE_TOPIC_JSON)

      val str = IOUtils.toString(in, "UTF-8")
      val map = JsonUtil.mapFromJson(str)
      val iterator = map.entrySet().iterator()
      var mapResult = new scala.collection.mutable.HashMap[String, String]
      while (iterator.hasNext) {
        val entry = iterator.next()
        mapResult.put(entry.getKey, entry.getValue.asInstanceOf[String])
      }
      mapResult
    } finally {
      IOUtils.closeQuietly(in)
    }

  }

  def distribute(msgs: ListBuffer[ZGMessage]): Unit = {
    msgs.groupBy(m => m.appId + "_" + m.zgid).foreach(msg => {
      msg._2.foreach(t => {
        if (t.result != -1) {
          if (!"[]".equals(t.data.get("data").toString)) {
            putIdInfos(t.data)
            val json = JsonUtil.toJson(t.data);
            simpleSend(new ProducerRecord[String, String](typeTopicMap("total"), String.valueOf(t.data.get("ak")), json))
            simpleSend(new ProducerRecord[String, String](typeTopicMap("total_random"), String.valueOf(t.appId) + "_" + t.zgid, json))
          }
        }
        if (null != t.errData && t.errData.containsKey("data")) {
          sendErrorMsgs(t)
        }
      })
    })
  }


  def sendErrorMsgs(msg: ZGMessage): Unit = {
    //有异常
    val errorMsgMap = new java.util.HashMap[String, Object]()
    var errorCode: Int = msg.errorCode
    var errorDescribe: String = msg.errorDescribe
    if (errorCode != ErrorMessageEnum.JSON_FORMAT_ERROR.getErrorCode && errorCode != ErrorMessageEnum.BASIC_SCHEMA_FORMAT_NOT_MATCH.getErrorCode && errorCode != ErrorMessageEnum.AK_NONE.getErrorCode) {
      val appId = msg.appId
      if (null == appId) {
        return;
      }
      val pl: String = String.valueOf(msg.data.get("pl"))
      val sdk: String = String.valueOf(msg.data.get("sdk"))
      val plat = Dims.sdk(pl)
      val dataItems = msg.errData.get("data").asInstanceOf[util.List[util.Map[String, Object]]].iterator()
      //异常数量自增写入ssdb
      val errorRedisKey = error_count_key + "#" + appId + ":" + getDay + ":" + plat + ":"
      //属性异常数量自增写入ssdb
      val errorProRedisKey = error_pro_count_key + "#" + appId + ":" + getDay + ":" + plat + ":"
      val jsonCount = new JSONObject();
      while (dataItems.hasNext) {
        val dataMap = new java.util.HashMap[String, Any]()
        val dataItem = dataItems.next()
        val dt = dataItem.get("dt").asInstanceOf[String]
        //处理带有上报的事件
        if ("evt".equals(dt) || "vtl".equals(dt) || "abp".equals(dt)) {
          try {
            val props = dataItem.get("pr").asInstanceOf[java.util.Map[String, Object]]
            //事件名称
            val eventName = if (String.valueOf(props.get("$eid")) == null) "" else String.valueOf(props.get("$eid"))
            //事件时间
            val ct = (String.valueOf(if (props.get("$ct") == null) 0 else props.get("$ct"))).toLong
            if (dataItem.containsKey("errorCode")) {
              errorCode = Integer.parseInt(dataItem.get("errorCode").toString)
              dataItem.remove("errorCode")
            }
            if (dataItem.containsKey("errorDescribe")) {
              errorDescribe = dataItem.get("errorDescribe").toString
              dataItem.remove("errorDescribe")
            }
            var proFlag = 0
            if (errorCode == ErrorMessageEnum.EVENT_ATTR_ID_ERROR.getErrorCode) {
              proFlag = 1
            }
            if (proFlag == 1) {
              if (jsonCount.containsKey(errorProRedisKey + eventName)) {
                jsonCount.put(errorProRedisKey + eventName, jsonCount.getIntValue(errorProRedisKey + eventName) + 1)
              } else {
                jsonCount.put(errorProRedisKey + eventName, 1)
              }
            } else {
              if (jsonCount.containsKey(errorRedisKey + eventName)) {
                jsonCount.put(errorRedisKey + eventName, jsonCount.getIntValue(errorRedisKey + eventName) + 1)
              } else {
                jsonCount.put(errorRedisKey + eventName, 1)
              }
            }
            val errData = msg.errData
            errData.put("data", dataItem)
            dataMap.put("app_id", appId)
            dataMap.put("error_code", errorCode)
            val errDataJson = JsonUtil.toJson(errData)
            dataMap.put("data_json", errDataJson)
            dataMap.put("data_md5", ToolUtil.getMD5Str(errDataJson))
            dataMap.put("error_md5", ToolUtil.getMD5Str(errorDescribe))
            dataMap.put("log_utc_date", System.currentTimeMillis())
            dataMap.put("log_utc_day_id", getDay)
            dataMap.put("event_begin_date", ct)
            dataMap.put("pl", pl)
            dataMap.put("sdk", sdk)
            dataMap.put("platform", plat)
            // 1表示事件属性相关错误日志标识,0非事件属性的错误标识
            dataMap.put("pro_flag", proFlag)
            dataMap.put("event_name", eventName)
            dataMap.put("error_msg", errorDescribe)
            errorMsgMap.put("data", dataMap)
            errorMsgMap.put("type", error_Log_Type)
            val record = new ProducerRecord[String, String](typeTopicMap("data_quality"), JsonUtil.toJson(errorMsgMap))
            simpleSend(record)
          } catch {
            case e: Exception => {
              println(e)
            }
          }
        }
      }
      sendCount(jsonCount, data_count_type)
    }
  }

  def sendCount(jsonCount: JSONObject, types: String) = {
    if (null != jsonCount && !jsonCount.isEmpty) {
      val allCountJson = new JSONObject()
      allCountJson.put("data", jsonCount)
      allCountJson.put("type", types)
      simpleSend(new ProducerRecord[String, String](typeTopicMap("data_quality"), allCountJson.toJSONString))
    }
  }

  def simpleSend(record: ProducerRecord[String, String]): Future[RecordMetadata] = {
    if (producerSingles == 1) {
      producerMap(0).send(record);
    } else {
      val mod = random.nextInt(producerSingles);
      producerMap(mod).send(record);
    }
  }

  def getDay: Integer = {
    val now: Date = new Date()
    val dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyyMMdd")
    val day = dateFormat.format(now)
    Integer.parseInt(day)
  }


  /**
    * return all type msg
    *
    * @param msgData
    */
  def putIdInfos(msgData: java.util.Map[String, java.lang.Object]): Unit = {

    val dataList = msgData.get("data").asInstanceOf[java.util.List[java.util.Map[String, java.lang.Object]]]

    //zgid
    val zgidItems = getIdInfosFromMsg(msgData)

    dataList.addAll(zgidItems);

    //did-pl
    val didItems = getDidInfosFromMsg(msgData);
    dataList.addAll(didItems);

    //uid-user
    val uidItems = getUidInfosFromMsg(msgData);
    dataList.addAll(uidItems);
  }


  def getDidInfosFromMsg(msgData: java.util.Map[String, java.lang.Object]): java.util.List[java.util.Map[String, Object]] = {
    val resultItem = new java.util.HashMap[String, Object]();
    resultItem.put("dt", "pl")
    val pr = new java.util.HashMap[String, Object]();
    resultItem.put("pr", pr)
    pr.put("$dv", "zhuge.io")
    pr.put("$zg_did", msgData.get("usr").asInstanceOf[java.util.Map[String, Object]].get("$zg_did"))

    val items = new util.ArrayList[java.util.Map[String, Object]]();
    items.add(resultItem)
    return items;
  }

  def getIdInfosFromMsg(msgData: java.util.Map[String, java.lang.Object]): java.util.List[java.util.Map[String, Object]] = {

    val lists = new util.ArrayList[java.util.Map[String, Object]]();
    val composits = new util.ArrayList[IdComposite]()

    val iterator = msgData.get("data").asInstanceOf[java.util.List[java.util.Map[String, java.lang.Object]]].iterator
    while (iterator.hasNext) {
      val map = iterator.next
      val idcomposite = getIdCompositeFromProp(map.get("pr").asInstanceOf[java.util.Map[String, Object]]);
      if (composits.size() > 0 && composits.get(composits.size() - 1) == idcomposite) {
        ;
      } else {
        composits.add(idcomposite)
        lists.add(getMapFromMap("zgid", Set("$ct", "$tz", "$zg_did", "$zg_uid", "$zg_zgid"), map.get("pr").asInstanceOf[java.util.Map[String, Object]]))
      }
    }
    return lists;
  }

  def getUidInfosFromMsg(msgData: java.util.Map[String, java.lang.Object]): java.util.List[java.util.Map[String, Object]] = {

    val lists = new util.ArrayList[java.util.Map[String, Object]]();
    val cuids = new java.util.HashSet[String]();

    val iterator = msgData.get("data").asInstanceOf[java.util.List[java.util.Map[String, java.lang.Object]]].iterator
    while (iterator.hasNext) {
      val map = iterator.next
      val dtype = map.get("dt").asInstanceOf[String]
      val pr = map.get("pr").asInstanceOf[java.util.Map[String, Object]];

      if (pr.containsKey("$cuid")) {
        val cuid = String.valueOf(pr.get("$cuid"));
        if (!cuids.contains(cuid)) {
          lists.add(getMapFromMap("usr", Set("$ct", "$tz", "$zg_did", "$zg_uid", "$zg_zgid", "$cuid"), map.get("pr").asInstanceOf[java.util.Map[String, Object]]));
          cuids.add(cuid)
        }
      }
    }
    return lists;
  }


  def getIdCompositeFromProp(prop: java.util.Map[String, Object]): IdComposite = {
    val zgDid: Long = if (prop.get("$zg_did") == null) null.asInstanceOf[java.lang.Long] else String.valueOf(prop.get("$zg_did")).toLong
    val zgUid: Long = if (prop.get("$zg_uid") == null) null.asInstanceOf[java.lang.Long] else String.valueOf(prop.get("$zg_uid")).toLong
    val zgId: Long = if (prop.get("$zg_zgid") == null) null.asInstanceOf[java.lang.Long] else String.valueOf(prop.get("$zg_zgid")).toLong
    IdComposite(zgDid, zgUid, zgId)
  }

  case class IdComposite(val zgDid: java.lang.Long, var zgUid: java.lang.Long, val zgId: java.lang.Long) {
  }

  def getMapFromMap(dt: String, keys: Set[String], prop: java.util.Map[String, Object]): java.util.Map[String, Object] = {
    val result = new java.util.HashMap[String, Object]();
    result.put("dt", dt);

    val resultProp = new java.util.HashMap[String, Object]();
    val iterator = prop.entrySet().iterator();
    while (iterator.hasNext) {
      val entry = iterator.next()

      if (keys.contains(entry.getKey) && prop.get(entry.getKey) != null) {
        resultProp.put(entry.getKey, entry.getValue)
      }
    }
    result.put("pr", resultProp)
    result
  }

}
