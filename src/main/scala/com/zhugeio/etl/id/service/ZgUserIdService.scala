package com.zhugeio.etl.id.service

import com.zhugeio.etl.id.ZGMessage
import com.zhugeio.etl.id.redis.UidRedisClient
import org.apache.commons.lang.StringUtils

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
  * Created by ziwudeng on 9/13/16.
  */
object ZgUserIdService {

  /**
    * 给消息加上诸葛用户ID
    *
    * @param msgs
    */
  def handleUserids(msgs: ListBuffer[ZGMessage]): Unit = {

    //userIds,format   u:${appId}:${userId}
    var userIds = mutable.HashSet[String]()
    var appIds = mutable.HashSet[String]()
    var userIdMap = mutable.HashMap[String, String]()

    //get userIds


    msgs.foreach(msg => {
      if (msg.result != -1) {
        val appId = msg.appId
        appIds += String.valueOf(appId)
        val iterator = msg.data.get("data").asInstanceOf[java.util.List[java.util.Map[String, java.lang.Object]]].iterator()
        while (iterator.hasNext) {
          val props = iterator.next().get("pr").asInstanceOf[java.util.Map[String, java.lang.Object]]
          if (props.containsKey("$cuid") && props.get("$cuid") != null && StringUtils.isNotBlank(String.valueOf(props.get("$cuid")))) {
            val cuid = String.valueOf(props.get("$cuid")).trim();
            userIds += "u:" + appId + ":" + cuid;
            props.put("$cuid", cuid)
          } else {
            props.remove("$cuid")
          }
        }
      }
    })

    userIdMap = UidRedisClient.getMap(userIds, true)

    var appMaxZgUidMap = UidRedisClient.getMaxMap(appIds.map(appId => {
      "id:u:" + appId
    }))

    //get zg_userId from redis
    UidRedisClient.doInJedis(jedis => {
      val pipline = jedis.pipelined()

      //inc zgUserId
      userIdMap.foreach(r => {
        val userId = r._1
        var zgUserId = r._2
        if (zgUserId == null) {
          var split = userId.split(":", 3)
          var key = split(0) + ":" + split(1)
          var hkey = split(2)
          var appId = split(1)
          var maxId = appMaxZgUidMap(appId)
          var currentId = String.valueOf(maxId + 1)
          userIdMap.put(userId, currentId)
          appMaxZgUidMap.put(appId, java.lang.Long.parseLong(currentId))
          pipline.hset(key, hkey, currentId)
          pipline.incr("id:u:" + appId)
        }
      })
      pipline.sync()
      null
    });

    msgs.foreach(msg => {
      if (msg.result != -1) {
        val appId = msg.appId
        val iterator = msg.data.get("data").asInstanceOf[java.util.List[java.util.Map[String, java.lang.Object]]].iterator()
        while (iterator.hasNext) {
          val pr = iterator.next().get("pr").asInstanceOf[java.util.Map[String, AnyVal]]
          if (pr.containsKey("$cuid")) {
            val userId = pr.get("$cuid")
            val zgUserId = userIdMap("u:" + appId + ":" + userId)
            pr.put("$zg_uid", java.lang.Long.parseLong(zgUserId))
          }
        }
      }
    });

  }
}
