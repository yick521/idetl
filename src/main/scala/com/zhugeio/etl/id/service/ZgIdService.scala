package com.zhugeio.etl.id.service

import java.lang
import java.util.regex.Pattern

import com.zhugeio.etl.commons.ErrorMessageEnum
import com.zhugeio.etl.id.ZGMessage
import com.zhugeio.etl.id.log.IdLogger
import com.zhugeio.etl.id.redis.RedisClient

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
  * Created by ziwudeng on 9/13/16.
  */
object ZgIdService {


  /**
    * 给消息加上诸葛ID
    *
    * @param msgs
    */
  def handleZgIds(msgs: ListBuffer[ZGMessage]): Unit = {

    //format dz:${appId}:${zgDid}
    var zgDids = mutable.HashSet[String]();

    //format uz:${appId}:${zgUid}
    var zgUids = mutable.HashSet[String]();

    //format ${appId}
    var appIds = mutable.HashSet[String]()


    //format d:${appId}:${zgDid}  ${zgId}
    var zgDidZgidMap = mutable.HashMap[String, String]()

    //format u:${appId}:${zgUid}  ${zgId}
    var zgUidZgidMap = mutable.HashMap[String, String]()

    //format z:${appId}:${zgId}  ${zgUid}
    var zgIdZgUidMap = mutable.HashMap[String, String]()

    val mkt = "mkt"
    val meid = "send"

    //get zgdids,zguids,appids
    msgs.foreach(msg => {
      if (msg.result != -1) {
        val appId = msg.appId
        appIds += String.valueOf(appId)
        val iterator = msg.data.get("data").asInstanceOf[java.util.List[java.util.Map[String, java.lang.Object]]].iterator()
        while (iterator.hasNext) {
          val props = iterator.next().get("pr").asInstanceOf[java.util.Map[String, java.lang.Object]]

          if (props.containsKey("$zg_did")) {
            zgDids += "dz:" + appId + ":" + props.get("$zg_did").asInstanceOf[lang.Long];
          }
          if (props.containsKey("$zg_uid")) {
            zgUids += "uz:" + appId + ":" + props.get("$zg_uid").asInstanceOf[lang.Long];
          }
          if (props.containsKey("$zg_fuid")) {
            zgUids += "uz:" + appId + ":" + props.get("$zg_fuid").asInstanceOf[lang.Long];
          }

        }
      }
    })

    //uz:appid:zg_uid  zg_uid
    zgUidZgidMap = RedisClient.getMap(zgUids, false)

    zgDidZgidMap = RedisClient.getMap(zgDids, false)


    //get zgIdZgUidMap
    val zgIds = mutable.HashSet() ++ zgDidZgidMap.map(r => {
      val split = r._1.split(":")
      val appId = split(1)
      "zu:" + appId + ":" + r._2
    }) ++ zgUidZgidMap.map(r => {
      val split = r._1.split(":")
      val appId = split(1)
      "zu:" + appId + ":" + r._2
    })

    zgIdZgUidMap = RedisClient.getMap(zgIds, false)

    var appMaxZgIdMap = RedisClient.getMaxMap(appIds.map(appId => {
      "id:z:" + appId
    }))

    //get maps from redis
    RedisClient.doInJedis(jedis => {
      val pipline = jedis.pipelined()

      //put zgid
      msgs.foreach(msg => {
        if (msg.result != -1) {
          val appId = String.valueOf(msg.appId)
          val iterator = msg.data.get("data").asInstanceOf[java.util.List[java.util.Map[String, java.lang.Object]]].iterator()
          while (iterator.hasNext) {
            val item = iterator.next()
            val props = item.get("pr").asInstanceOf[java.util.Map[String, java.lang.Object]]
            val zgDid = String.valueOf(props.get("$zg_did"))
            val zgUid = String.valueOf(props.get("$zg_uid"))
            val zgFuid = String.valueOf(props.get("$zg_fuid"))
            val zgUidKey = s"uz:${appId}:${zgUid}"
            val zgFuidKey = s"uz:${appId}:${zgFuid}"
            val zgDidKey = s"dz:${appId}:${zgDid}"
            var zgId: String = null
            var zgFid: String = null

            if (zgUid != "null") {
              if (zgUidZgidMap.contains(zgUidKey)) {
                zgId = zgUidZgidMap(zgUidKey)
                if (zgDidZgidMap.contains(zgDidKey) && zgDidZgidMap(zgDidKey) == zgId) {
                  ;
                } else {
                  zgDidZgidMap.put(zgDidKey, zgId)
                  pipline.hset(s"dz:${appId}", zgDid, zgId)
                }
              } else {
                if (zgDidZgidMap.contains(zgDidKey) && !zgIdZgUidMap.contains(s"zu:${appId}:" + zgDidZgidMap(zgDidKey))) {
                  zgId = zgDidZgidMap(zgDidKey)
                  pipline.hset(s"uz:${appId}", zgUid, zgId)
                  pipline.hset(s"zu:${appId}", zgId, zgUid)
                  zgUidZgidMap.put(zgUidKey, zgId)
                  zgIdZgUidMap.put(s"zu:${appId}:${zgId}", zgUid)
                } else {
                  val maxId = appMaxZgIdMap(appId)
                  zgId = String.valueOf(maxId + 1)

                  zgDidZgidMap.put(zgDidKey, zgId)
                  zgUidZgidMap.put(zgUidKey, zgId)
                  zgIdZgUidMap.put(s"zu:${appId}:${zgId}", zgUid)
                  appMaxZgIdMap.put(appId, java.lang.Long.parseLong(zgId))

                  pipline.hset(s"dz:${appId}", zgDid, zgId)
                  pipline.hset(s"uz:${appId}", zgUid, zgId)
                  pipline.hset(s"zu:${appId}", zgId, zgUid)
                  pipline.incr("id:z:" + appId)
                }
              }
            }
            else {
              val dt = item.get("dt")
              var eid = ""
              if (mkt.equals(dt)) {
                eid = props.get("$eid").toString
                if (null == eid || eid.isEmpty) {
                  msg.result = -1
                  msg.errorCode += ErrorMessageEnum.EID_NONE.getErrorCode
                  val errorInfo = ErrorMessageEnum.EID_NONE.getErrorMessage
                  //$eid获取异常
                  msg.errorDescribe += errorInfo
                  IdLogger.errorFormat(errorInfo, msg.rawData)
                }
              }
              if (mkt.equals(dt) && eid.equals(meid)) {
                val usr = msg.data.get("usr").asInstanceOf[java.util.Map[String, java.lang.Object]]
                val did = usr.get("did").toString
                zgId = did.split("_")(1)
                val pattern = Pattern.compile("[0-9]*")
                val flag = pattern.matcher(zgId).matches()
                if (flag) {
                  zgDidZgidMap.put(zgDidKey, zgId)
                  appMaxZgIdMap.put(appId, java.lang.Long.parseLong(zgId))
                  pipline.hset(s"dz:${appId}", zgDid, zgId)
                } else {
                  msg.result = -1
                  msg.errorCode += ErrorMessageEnum.MKT_SEND_ZG_ID_NONE.getErrorCode
                  val errorInfo = ErrorMessageEnum.MKT_SEND_ZG_ID_NONE.getErrorMessage
                  //触达事件获取zg_id异常
                  msg.errorDescribe +=errorInfo
                  IdLogger.errorFormat(errorInfo, msg.rawData)
                }
              } else if (zgDidZgidMap.contains(zgDidKey)) {
                zgId = zgDidZgidMap(zgDidKey)
              } else {
                val maxId = appMaxZgIdMap(appId)
                zgId = String.valueOf(maxId + 1)
                zgDidZgidMap.put(zgDidKey, zgId)
                appMaxZgIdMap.put(appId, java.lang.Long.parseLong(zgId))
                pipline.hset(s"dz:${appId}", zgDid, zgId)
                pipline.incr("id:z:" + appId)
              }
              if (zgIdZgUidMap.contains(s"zu:${appId}:${zgId}")) {
                props.put("$zg_uid", new lang.Long(java.lang.Long.parseLong(zgIdZgUidMap(s"zu:${appId}:${zgId}"))))
              }
            }
            props.put("$zg_zgid", new lang.Long(lang.Long.parseLong(zgId)))
            msg.zgid = new lang.Long(java.lang.Long.parseLong(zgId))

            if (zgFuid != "null") {
              if (zgUidZgidMap.contains(zgFuidKey)) {
                zgFid = zgUidZgidMap(zgFuidKey)
                if (zgDidZgidMap.contains(zgDidKey) && zgDidZgidMap(zgDidKey) == zgFid) {
                  ;
                } else {
                  zgDidZgidMap.put(zgDidKey, zgFid)
                  pipline.hset(s"dz:${appId}", zgDid, zgFid)
                }
              } else {
                if (zgDidZgidMap.contains(zgDidKey) && !zgIdZgUidMap.contains(s"zu:${appId}:" + zgDidZgidMap(zgDidKey))) {
                  zgFid = zgDidZgidMap(zgDidKey)
                  pipline.hset(s"uz:${appId}", zgFuid, zgFid)
                  pipline.hset(s"zu:${appId}", zgFid, zgFuid)
                  zgUidZgidMap.put(zgFuidKey, zgFid)
                  zgIdZgUidMap.put(s"zu:${appId}:${zgFid}", zgUid)
                } else {
                  val maxId = appMaxZgIdMap(appId)
                  zgFid = String.valueOf(maxId + 1)

                  zgDidZgidMap.put(zgDidKey, zgFid)
                  zgUidZgidMap.put(zgFuidKey, zgFid)
                  zgIdZgUidMap.put(s"zu:${appId}:${zgFid}", zgFuid)
                  appMaxZgIdMap.put(appId, java.lang.Long.parseLong(zgFid))

                  pipline.hset(s"dz:${appId}", zgDid, zgFid)
                  pipline.hset(s"uz:${appId}", zgFuid, zgFid)
                  pipline.hset(s"zu:${appId}", zgFid, zgFuid)
                  pipline.incr("id:z:" + appId)
                }
              }
              props.put("$zg_zgfid", new lang.Long(lang.Long.parseLong(zgFid)))
            }
          }
        }
      })
      pipline.sync()
      null
    })


  }
}
