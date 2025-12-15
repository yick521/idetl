package com.zhugeio.etl.id.service

import com.zhugeio.etl.commons.ErrorMessageEnum
import com.zhugeio.etl.id.ZGMessage
import com.zhugeio.etl.id.commons.CaffeineCache
import com.zhugeio.etl.id.log.IdLogger
import com.zhugeio.etl.id.redis.DidRedisClient
import org.apache.commons.lang.StringUtils

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
  * Created by ziwudeng on 9/13/16.
  */
object ZgDeviceIdService {

  /**
    * 给消息加上诸葛设备ID
    *
    * @param msgs
    */
  def handleDeviceids(msgs: ListBuffer[ZGMessage],a: Int): Unit = {
//    val a = new java.util.Random().nextInt(10000)
    //deviceIds,format   d:${appId}:${deviceId}
    var deviceIds = mutable.HashSet[String]()
    var appIds = mutable.HashSet[String]()
    var deviceIdMap = mutable.HashMap[String, String]()
    val msgsize = msgs.size
//    val time0= System.currentTimeMillis()
    //get deviceIds
    msgs.foreach(msg => {
      if (msg.result != -1) {
        val appId = msg.appId
        appIds += String.valueOf(appId)
        if (StringUtils.isBlank(msg.data.get("usr").asInstanceOf[java.util.Map[String, AnyRef]].get("did").asInstanceOf[String])) {
          msg.result = -1;
          msg.error = "did is blank"
          msg.errorCode += ErrorMessageEnum.DID_NONE.getErrorCode
          //did获取异常
          val errorInfo =ErrorMessageEnum.DID_NONE.getErrorMessage
          msg.errorDescribe += errorInfo
          IdLogger.errorFormat(errorInfo, msg.rawData)
        } else {
          deviceIds += "d:" + appId + ":" + msg.data.get("usr").asInstanceOf[java.util.Map[String, AnyRef]].get("did").asInstanceOf[String];
        }
      }
    })
//    val time1= System.currentTimeMillis()
    deviceIdMap  = new collection.mutable.HashMap[String,String]
    val notInCahcedeviceIds:mutable.HashSet[String] = CaffeineCache.getDeviceMap(deviceIds,deviceIdMap)
    val notInCacheSize = if(notInCahcedeviceIds==null)0 else if(notInCahcedeviceIds.isEmpty)0 else notInCahcedeviceIds.size
    val time2= System.currentTimeMillis()
//      IdLogger.info(s"handleDeviceids_getFromCaffeine,a:${a},time:"+(time2-time1)+",deviceIds="+deviceIds.size)
    val notInSSDBSet = DidRedisClient.getMap(notInCahcedeviceIds, deviceIdMap)
    val notInSSDBSize = if(notInSSDBSet==null) 0 else if (notInSSDBSet.isEmpty) 0 else notInSSDBSet.size
    val time3= System.currentTimeMillis()
    val didCacheSize = CaffeineCache.getdidCacheSize()
    if((time3-time2)>10000) {
      IdLogger.info(s"handleDeviceids_getfromSsdb,a:${a},time:" + (time3 - time2) + ",didCacheSize:" + didCacheSize + ",appIds:" + appIds + s",deviceIds:${deviceIds.size},notInCacheSize:" + notInCacheSize + ",notInSSDBSize:" + notInSSDBSize)
    }else {
      IdLogger.info(s"handleDeviceids_getfromSsdb,a:${a},time:" + (time3 - time2) + ",didCacheSize:" + didCacheSize + ",appId_size:" + appIds.size + s",deviceIds:${deviceIds.size},notInCacheSize:" + notInCacheSize + ",notInSSDBSize:" + notInSSDBSize)
    }

      if(notInSSDBSize>0) {
      val appMaxZgDidMap = DidRedisClient.getMaxMap(appIds.map(appId => {
        "id:d:" + appId
    }))
//    val time4= System.currentTimeMillis()
//      IdLogger.info(s"handleDeviceids_getMaxMap,a:${a},time:"+(time4-time3)+",appIds="+appIds.size)


    //get zg_deviceId from redis
    DidRedisClient.doInJedis(jedis => {
      val pipline = jedis.pipelined()

      //inc zgDeviceId
        notInSSDBSet.foreach(deviceId => {
          var split = deviceId.split(":", 3)
          var key = split(0) + ":" + split(1)
          var hkey = split(2)
          var appId = split(1)
          var maxId = appMaxZgDidMap(appId)
          var currentId = String.valueOf(maxId + 1)
          deviceIdMap.put(deviceId, currentId)
          CaffeineCache.putDeviceCache(deviceId, currentId)
          appMaxZgDidMap.put(appId, java.lang.Long.parseLong(currentId))
          pipline.hset(key, hkey, currentId)
          pipline.incr("id:d:" + appId)
        })

        //pipline.syncAndReturnAll()
        pipline.sync()
        null
      })
    }

//    val time5= System.currentTimeMillis()
//    IdLogger.info(s"handleDeviceids_SSDBINCR,a:${a},time:"+(time5-time4)+",notInSSDBMap="+notInSSDBMap.size)
    msgs.foreach(msg => {
      if (msg.result != -1) {
        val appId = msg.appId
        val deviceId = "d:" + appId + ":" + msg.data.get("usr").asInstanceOf[java.util.Map[String, AnyRef]].get("did").asInstanceOf[String];
        val zgDeviceId = deviceIdMap(deviceId)
        msg.data.get("usr").asInstanceOf[java.util.Map[String, AnyVal]].put("$zg_did", java.lang.Long.parseLong(zgDeviceId))
        val iterator = msg.data.get("data").asInstanceOf[java.util.List[java.util.Map[String, AnyVal]]].iterator()
        while (iterator.hasNext) {
          val pr = iterator.next().get("pr").asInstanceOf[java.util.Map[String, AnyVal]]
          if (pr != null) {
            pr.put("$zg_did", java.lang.Long.parseLong(zgDeviceId));
          }
        }
      }
    })
  }
}
