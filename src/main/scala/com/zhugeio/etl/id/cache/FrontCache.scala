package com.zhugeio.etl.id.cache

import java.util.concurrent.ConcurrentHashMap
import java.util.{Collections, Date}
import com.zhugeio.etl.id.Config
import com.zhugeio.etl.id.adtoufang.common.AdsLinkEvent
import com.zhugeio.etl.id.cache.FrontCache.{handleEventVirtualAttrIdsSet, handleVirtualEventPropMap, handleVirtualPropAppIdsSet, handleVirtualUserPropMap}
import com.zhugeio.etl.id.dao.FrontDao
import com.zhugeio.etl.id.service.MainService.openToufang
import org.slf4j.{Logger, LoggerFactory}

import java.util
import java.util.concurrent.atomic.AtomicLong
import scala.collection.JavaConverters._
import scala.collection.mutable

/**
  * Created by ziwudeng on 10/17/16.
  */
object FrontCache {

  /**
    * <${appKey},${appId}>
    */
  @volatile
  var appKeyAppIdMap = new ConcurrentHashMap[String, Integer]().asScala

  @volatile
  var appIdSdkHasDataMap = new ConcurrentHashMap[String, Integer]().asScala

  @volatile
  var appIdPropIdMap = new ConcurrentHashMap[String, Integer]().asScala

  @volatile
  var appIdPropIdOriginalMap = new ConcurrentHashMap[String, String]().asScala

  @volatile
  var appIdEventIdMap = new ConcurrentHashMap[String, Integer]().asScala

  @volatile
  var appIdEventAttrIdMap = new ConcurrentHashMap[String, Integer]().asScala

  @volatile
  var appIdDevicePropIdMap = new ConcurrentHashMap[String, Integer]().asScala

  @volatile
  var appIdCreateEventForbidSet = Collections.newSetFromMap(new ConcurrentHashMap[Integer, java.lang.Boolean]()).asScala

  @volatile
  var appIdUploadDataSet = Collections.newSetFromMap(new ConcurrentHashMap[Integer, java.lang.Boolean]()).asScala

  @volatile
  var blackUserPropSet = Collections.newSetFromMap(new ConcurrentHashMap[Integer, java.lang.Boolean]()).asScala

  @volatile
  var blackEventIdSet = Collections.newSetFromMap(new ConcurrentHashMap[Integer, java.lang.Boolean]()).asScala

  @volatile
  var blackEventNameSet = Collections.newSetFromMap(new ConcurrentHashMap[String, java.lang.Boolean]()).asScala

  @volatile
  var blackEventAttrNameSet = Collections.newSetFromMap(new ConcurrentHashMap[String, java.lang.Boolean]()).asScala

  @volatile
  var appIdNoneAutoCreateSet = Collections.newSetFromMap(new ConcurrentHashMap[Integer, java.lang.Boolean]()).asScala

  @volatile
  var blackEventAttrIdSet = Collections.newSetFromMap(new ConcurrentHashMap[Integer, java.lang.Boolean]()).asScala

  @volatile
  var eventIdCreateAttrForbiddenSet = Collections.newSetFromMap(new ConcurrentHashMap[Integer, java.lang.Boolean]()).asScala

  @volatile
  var eventIdPlatform = Collections.newSetFromMap(new ConcurrentHashMap[String, java.lang.Boolean]()).asScala

  @volatile
  var eventAttrdPlatform = Collections.newSetFromMap(new ConcurrentHashMap[String, java.lang.Boolean]()).asScala

  @volatile
  var virtualEventPropMap = new ConcurrentHashMap[String, java.util.ArrayList[String]]().asScala
  @volatile
  var virtualUserPropMap = new ConcurrentHashMap[String, java.util.ArrayList[String]]().asScala
  @volatile
  var virtualPropAppIdsSet = Collections.newSetFromMap(new ConcurrentHashMap[String, java.lang.Boolean]()).asScala
  @volatile
  var eventVirtualAttrIdsSet = Collections.newSetFromMap(new ConcurrentHashMap[String, java.lang.Boolean]()).asScala



  @volatile
  var devicePropPlatform = Collections.newSetFromMap(new ConcurrentHashMap[String, java.lang.Boolean]()).asScala

  @volatile
  var appIdSMap = new ConcurrentHashMap[Integer, Integer]().asScala
  //用户该应用是否开启广告投放
  @volatile
  var openAdvertisingFunctionAppMap = new ConcurrentHashMap[String, Integer]().asScala

  @volatile
  var lidAndChannelEventMap = new ConcurrentHashMap[String, String]().asScala

  @volatile
  var adFrequencySet = Collections.newSetFromMap(new ConcurrentHashMap[String, java.lang.Boolean]()).asScala

  @volatile
  var virtualEventMap = new ConcurrentHashMap[String, java.util.ArrayList[String]]().asScala
  @volatile
  var virtualEventAttrMap = new ConcurrentHashMap[String, Set[String]]().asScala
  @volatile
  var eventAttrAliasMap = new ConcurrentHashMap[String, String]().asScala
  @volatile
  var virtualEventAppidsSet = Collections.newSetFromMap(new ConcurrentHashMap[String, java.lang.Boolean]()).asScala
  @volatile
  var adsLinkEventMap = new ConcurrentHashMap[String, AdsLinkEvent]().asScala
  //event 最后更新时间
//  @volatile
//  var eventlastInsertTime: mutable.HashMap[Int, String] = mutable.HashMap()
  @volatile
  var eventlastInsertTime = new ConcurrentHashMap[Int, String]().asScala

  @volatile
  var idUpdateTime = new Date()

  val idTimeout = Config.getInt(Config.ID_TIMEOUT_SECONDS)


  @volatile
//  var updateTime = new Date();
  @volatile
  var atomicLong = new AtomicLong(System.currentTimeMillis)

  val logger: Logger = LoggerFactory.getLogger(this.getClass)

  init()

//  val logger = Logger.getLogger(this.getClass)

  def init(): Unit = {

    try {
//      handleAppKeyAppIdMap
      startIDLoop
//      handleAll
      startLoop
      startCheckIdTimeOutLoop
      println("init front cache")
    } catch {
      case e: Exception => {
        logger.error("flush cache exception ", e)
        e.printStackTrace()
        throw e
      }
    }

  }

  def handleAll(): Unit = {
    handleAppIdSdkHasDataMap
    handleAppIdPropIdMap
    handleBlackUserPropIdSet
    handleAppIdEventIdMap
    handleAppIdEventAttrIdMap
    handleAppIdDevicePropIdMap
    handleEventAttrIdPlatform
    handleAppIdCreateEventForbidSet
    handleAppIdUploadDataSet
    handleBlackEventIdSet
    handleAppIdNoneAutoCreateSet
    handleBlackEventAttrIdSet
    handleEventIdCreateAttrForbiddenSet
    handleEventIdPlatform
    handleDevicePropIdPlatform
    handleVirtualEventMap
    handleVirtualEventAttrMap
    //改成每批次计算前同步
    handleEventAttrAliasMap
    //添加事件最后上传时间
    putEventLastInsertTime()
    handleVirtualEventAppidsSet
    if ("true".equals(openToufang)) {
      handleAdsLinkEventMap
      handleOpenAdvertisingFunctionAppId
      handleLidAndChannelEventMap
      handleAppIdMap
      handleAdFrequency
    }
    handleVirtualPropAppIdsSet
    //虚拟事件属性
    handleVirtualEventPropMap
    //虚拟用户属性
    handleVirtualUserPropMap
    handleEventVirtualAttrIdsSet
  }


  def startIDLoop: Unit = {
    val thread = new Thread(new Runnable {
      override def run(): Unit = {
        while (true) {
          logger.info("begin cache handleAppKeyAppIdMap")
          try {
            handleAppKeyAppIdMap
          } catch {
            case e: Exception => {
              logger.error("flush cache exception handleAppKeyAppIdMap ", e)
              e.printStackTrace()
              System.exit(1)
            }
          }
          logger.info("end cache")
          Thread.sleep(5000)
        }
      }
    });
    thread.setDaemon(true)
    thread.start()
  }

  def startLoop: Unit = {
    val thread = new Thread(new Runnable {
      override def run(): Unit = {
        while (true) {
          logger.info("begin cache")
          try {
            handleAll
          } catch {
            case e: Exception => {
              logger.error("flush cache exception ", e)
              e.printStackTrace()
              System.exit(1)
            }
          }
          logger.info("end cache")
          Thread.sleep(30000)
        }
      }
    });
    thread.setDaemon(true)
    thread.start()
  }

  def checkIdTimeout(): Boolean = {
    val now = new Date()
    Math.abs(now.getTime - idUpdateTime.getTime) > idTimeout * 1000
  }

  def startCheckIdTimeOutLoop(): Unit = {
    val thread = new Thread(new Runnable {
      override def run(): Unit = {
        while (true) {
          Thread.sleep(5000)
          logger.info("begin checkIdTimeOut ")
          if (checkIdTimeout()) {
            logger.error("id timeout :" + idUpdateTime.toString);
            System.err.println("id timeout");
            System.exit(1)
          }
          logger.info("end checkIdTimeOut normal ")
        }
      }
    });
    thread.setDaemon(true)
    thread.start()
  }


  def handleAppKeyAppIdMap(): Unit = {
    val appKeyAppIdMapDB = FrontDao.getAppKeyIdMaps()
    val appKeyAppIdMapTemp = new ConcurrentHashMap[String, Integer]().asScala
    appKeyAppIdMapDB.foreach(f => {
      appKeyAppIdMapTemp.put(f._1, f._2)
    })
    appKeyAppIdMap = appKeyAppIdMapTemp
    idUpdateTime = new Date()
  }

  def handleEventVirtualAttrIdsSet: Unit = {
    val eventVirtualAttrIds = FrontDao.getEventVirtualAttrIds()
    val eventVirtualAttrIdsSetTemp = Collections.newSetFromMap(new ConcurrentHashMap[String, java.lang.Boolean]()).asScala
    eventVirtualAttrIds.foreach(f => {
      eventVirtualAttrIdsSetTemp.add(f)
    })
    eventVirtualAttrIdsSet = eventVirtualAttrIdsSetTemp
  }

  def handleVirtualEventPropMap: Unit = {
    val virtualProp = FrontDao.getVirtualEventPropMap()
    val tmpVirtualPropMap = new ConcurrentHashMap[String, java.util.ArrayList[String]]().asScala
    virtualProp.foreach(f => {
      tmpVirtualPropMap.put(f._1, f._2)
    })
    virtualEventPropMap = tmpVirtualPropMap
  }

  def handleVirtualUserPropMap: Unit = {
    val virtualProp = FrontDao.getVirtualUserPropMap()
    val tmpVirtualPropMap = new ConcurrentHashMap[String, java.util.ArrayList[String]]().asScala
    virtualProp.foreach(f => {
      tmpVirtualPropMap.put(f._1, f._2)
    })
    virtualUserPropMap = tmpVirtualPropMap
  }


  def handleVirtualPropAppIdsSet: Unit = {
    val virtualEvent = FrontDao.getVirtualPropAppIdsSet
    val tmpVirtualProp = Collections.newSetFromMap(new ConcurrentHashMap[String, java.lang.Boolean]()).asScala
    virtualEvent.foreach(f => {
      tmpVirtualProp.add(f)
    })
    virtualPropAppIdsSet = tmpVirtualProp
  }

  def handleAppIdSdkHasDataMap: Unit = {
    val appIdSdkHasDataMapDB = FrontDao.getSdkPlatformHasDataMap()
    val appIdSdkHasDataMapTemp = new ConcurrentHashMap[String, Integer]().asScala
    appIdSdkHasDataMapDB.foreach(f => {
      appIdSdkHasDataMapTemp.put(f._1, f._2)
    })
    appIdSdkHasDataMap = appIdSdkHasDataMapTemp
  }


  def handleAppIdPropIdMap: Unit = {
    val appIdPropIdMapDB = FrontDao.getUserPropIds()
    val appIdPropIdMapOriginalDB = FrontDao.getOriginalUserPropIds()
    val appIdPropIdMapTemp = new ConcurrentHashMap[String, Integer]().asScala
    appIdPropIdMapDB.foreach(f => {
      appIdPropIdMapTemp.put(f._1, f._2)
    })
    val appIdPropIdOriginalMapTemp = new ConcurrentHashMap[String, String]().asScala
    appIdPropIdMapOriginalDB.foreach(f => {
      appIdPropIdOriginalMapTemp.put(f._1, f._2)
    })
    appIdPropIdMap = appIdPropIdMapTemp
    appIdPropIdOriginalMap = appIdPropIdOriginalMapTemp
  }

  def handleBlackUserPropIdSet(): Unit = {
    val blackSetDB = FrontDao.getBlackUserPropIds()
    val blackSetDBTemp = Collections.newSetFromMap(new ConcurrentHashMap[Integer, java.lang.Boolean]()).asScala
    blackSetDB.foreach(f => {
      blackSetDBTemp.add(f)
    })
    blackUserPropSet = blackSetDBTemp
  }

  def handleAppIdDevicePropIdMap: Unit = {
    val appIdPropIdMapDB = FrontDao.getDevicePropIds()
    val appIdPropIdMapTemp = new ConcurrentHashMap[String, Integer]().asScala
    appIdPropIdMapDB.foreach(f => {
      appIdPropIdMapTemp.put(f._1, f._2)
    })
    appIdDevicePropIdMap = appIdPropIdMapTemp
  }

  def handleAppIdEventIdMap: Unit = {
    val appIdEventIdMapDB = FrontDao.getEventIds()
    val appIdEventIdMapTemp = new ConcurrentHashMap[String, Integer]().asScala
    appIdEventIdMapDB.foreach(f => {
      appIdEventIdMapTemp.put(f._1, f._2)
    })
    appIdEventIdMap = appIdEventIdMapTemp
  }

  def handleAppIdEventAttrIdMap: Unit = {
    val appIdEventAttrIdMapDB = FrontDao.getEventAttrIds()
    val appIdEventAttrIdMapTemp = new ConcurrentHashMap[String, Integer]().asScala
    appIdEventAttrIdMapDB.foreach(f => {
      appIdEventAttrIdMapTemp.put(f._1, f._2)
    })
    appIdEventAttrIdMap = appIdEventAttrIdMapTemp
  }

  def handleAppIdCreateEventForbidSet: Unit = {
    val appIdCreateEventForbidSetDB = FrontDao.getForbiddenCreateEventAppIds()
    val appIdCreateEventForbidSetTemp = Collections.newSetFromMap(new ConcurrentHashMap[Integer, java.lang.Boolean]()).asScala
    appIdCreateEventForbidSetDB.foreach(f => {
      appIdCreateEventForbidSetTemp.add(f)
    })
    appIdCreateEventForbidSet = appIdCreateEventForbidSetTemp
  }

  def handleBlackEventIdSet: Unit = {
    val blackEventIdSetDB = FrontDao.getBlackEventIds()
    val blackEventIdSetTemp = Collections.newSetFromMap(new ConcurrentHashMap[Integer, java.lang.Boolean]()).asScala
    blackEventIdSetDB.foreach(f => {
      blackEventIdSetTemp.add(f)
    })
    blackEventIdSet = blackEventIdSetTemp
  }

  def handleAppIdUploadDataSet: Unit = {
    val appIdUploadDataSetDB = FrontDao.getUploadDatas()
    val appIdUploadDataSetTemp = Collections.newSetFromMap(new ConcurrentHashMap[Integer, java.lang.Boolean]()).asScala
    appIdUploadDataSetDB.foreach(f => {
      appIdUploadDataSetTemp.add(f)
    })
    appIdUploadDataSet = appIdUploadDataSetTemp
  }

  def handleAppIdNoneAutoCreateSet: Unit = {
    val appIdNoneAutoCreateSetDB = FrontDao.getNoneAutoCreateAppIds()
    val appIdNoneAutoCreateSetTemp = Collections.newSetFromMap(new ConcurrentHashMap[Integer, java.lang.Boolean]()).asScala
    appIdNoneAutoCreateSetDB.foreach(f => {
      appIdNoneAutoCreateSetTemp.add(f)
    })
    appIdNoneAutoCreateSet = appIdNoneAutoCreateSetTemp
  }

  def handleBlackEventAttrIdSet: Unit = {
    val blackEventAttrIdSetDB = FrontDao.getBlackEventAttrIds()
    val blackEventAttrIdSetTemp = Collections.newSetFromMap(new ConcurrentHashMap[Integer, java.lang.Boolean]()).asScala
    blackEventAttrIdSetDB.foreach(f => {
      blackEventAttrIdSetTemp.add(f)
    })
    blackEventAttrIdSet = blackEventAttrIdSetTemp
  }

  def handleEventIdCreateAttrForbiddenSet: Unit = {
    val eventIdCreateAttrForbiddenSetDB = FrontDao.getForbiddenCreateEventAttrEventIdsIds()
    val eventIdCreateAttrForbiddenSetTemp = Collections.newSetFromMap(new ConcurrentHashMap[Integer, java.lang.Boolean]()).asScala
    eventIdCreateAttrForbiddenSetDB.foreach(f => {
      eventIdCreateAttrForbiddenSetTemp.add(f)
    })
    eventIdCreateAttrForbiddenSet = eventIdCreateAttrForbiddenSetTemp
  }

  def handleEventIdPlatform: Unit = {
    val eventIdPlatformTempDB = FrontDao.getEventPlatforms();
    val eventIdPlatformTemp = Collections.newSetFromMap(new ConcurrentHashMap[String, java.lang.Boolean]()).asScala
    eventIdPlatformTempDB.foreach(f => {
      eventIdPlatformTemp.add(f)
    })
    eventIdPlatform = eventIdPlatformTemp
  }

  def handleEventAttrIdPlatform: Unit = {
    val eventIdPlatformTempDB = FrontDao.getEventAttrPlatforms()
    val eventIdPlatformTemp = Collections.newSetFromMap(new ConcurrentHashMap[String, java.lang.Boolean]()).asScala
    eventIdPlatformTempDB.foreach(f => {
      eventIdPlatformTemp.add(f)
    })
    eventAttrdPlatform = eventIdPlatformTemp
  }


  def handleDevicePropIdPlatform: Unit = {
    val eventIdPlatformTempDB = FrontDao.getDevicePropPlatforms()
    val eventIdPlatformTemp = Collections.newSetFromMap(new ConcurrentHashMap[String, java.lang.Boolean]()).asScala
    eventIdPlatformTempDB.foreach(f => {
      eventIdPlatformTemp.add(f)
    })
    devicePropPlatform = eventIdPlatformTemp
  }


  def getAppId(appKey: String): Option[Integer] = {
    if(appKeyAppIdMap.isEmpty){
      handleAppKeyAppIdMap()
    }
    appKeyAppIdMap.get(appKey)
  }

  def putAppIdKey(appKey: String, appId: Integer): Unit = {
    appKeyAppIdMap.putIfAbsent(appKey, appId)
  }

  def getHasData(appId: Integer, platform: Integer): Option[Integer] = {
    appIdSdkHasDataMap.get(s"${appId}_${platform}")
  }

  def putHasData(appId: Integer, platform: Integer): Unit = {
    appIdSdkHasDataMap.put(s"${appId}_${platform}", 1)
  }

  def getUserPropId(appId: Integer, owner: String, prop: String): Option[Integer] = {
    if(appIdPropIdMap.isEmpty){
      handleAppIdPropIdMap
    }
    appIdPropIdMap.get(s"${appId}_${owner}_${prop.toUpperCase}")
  }

  def putUserPropId(appId: Integer, owner: String, prop: String, propId: Integer) = {
    appIdPropIdMap.put(s"${appId}_${owner}_${prop.toUpperCase}", propId)
    appIdPropIdOriginalMap.put(s"${appId}_${owner}_${propId}", prop)
  }

  def getEventId(appId: Integer, owner: String, eventName: String): Option[Integer] = {
    if (appIdEventIdMap.isEmpty){
      handleAppIdEventIdMap
    }
    appIdEventIdMap.get(s"${appId}_${owner}_${eventName}")
  }

  def putEventIdMap(appId: Integer, owner: String, eventName: String, eventId: Integer) = {
    appIdEventIdMap.put(s"${appId}_${owner}_${eventName}", eventId)
  }

  def hasEventPlat(appId: Integer, eventId: Integer, platform: Integer): Boolean = {
    eventIdPlatform.contains(s"${eventId}_${platform}")
  }

  def putEventPlat(appId: Integer, eventId: Integer, platform: Integer): Unit = {
    eventIdPlatform.add(s"${eventId}_${platform}")
  }

  def getDevicePropId(appId: Integer, owner: String, propName: String): Option[Integer] = {
    appIdDevicePropIdMap.get(s"${appId}_${owner}_${propName}")
  }

  def putDevicePropIdMap(appId: Integer, owner: String, propName: String, propId: Integer) = {
    appIdDevicePropIdMap.put(s"${appId}_${owner}_${propName}", propId)
  }

  def isUserPropBlack(propId: Integer): Boolean = {
    blackUserPropSet.contains(propId)
  }

  def hasDevicePropPlat(appId: Integer, devicePropId: Integer, platform: Integer): Boolean = {
    devicePropPlatform.contains(s"${devicePropId}_${platform}")
  }

  def putDevicepropPlat(appId: Integer, devicePropId: Integer, platform: Integer): Unit = {
    devicePropPlatform.add(s"${devicePropId}_${platform}")
  }

  def isEventBlack(eventId: Integer): Boolean = {
    blackEventIdSet.contains(eventId)
  }

  def isAppIdCreateEventForbid(appId: Integer): Boolean = {
    appIdCreateEventForbidSet.contains(appId)
  }

  def putAppIdCreateEventForbid(appId: Integer): Unit = {
    appIdCreateEventForbidSet.add(appId)
  }

  def isAppIdAutoCreate(appId: Integer): Boolean = {
    !appIdNoneAutoCreateSet.contains(appId)
  }

  def getEventAttrId(appId: Integer, eventId: Integer, owner: String, attrName: String): Option[Integer] = {
    if(appIdEventAttrIdMap.isEmpty){
      handleAppIdEventAttrIdMap
    }
    appIdEventAttrIdMap.get(s"${appId}_${eventId}_${owner}_${attrName.toUpperCase}")
  }

  def putEventAttrId(appId: Integer, eventId: Integer, owner: String, attrName: String, id: Integer): Unit = {
    appIdEventAttrIdMap.put(s"${appId}_${eventId}_${owner}_${attrName.toUpperCase}", id)
  }

  def hasEventAttrPlat(appId: Integer, eventId: Integer, attrId: Integer, platform: Integer): Boolean = {
    eventAttrdPlatform.contains(s"${attrId}_${platform}")
  }

  def putEventAttrPlat(appId: Integer, eventId: Integer, attrId: Integer, platform: Integer): Unit = {
    eventAttrdPlatform.add(s"${attrId}_${platform}")
  }

  def isEventIdCreateEventAttrForbid(eventId: Integer): Boolean = {
    eventIdCreateAttrForbiddenSet.contains(eventId)
  }

  def putEventIdCreateEventAttrForbid(eventId: Integer): Unit = {
    eventIdCreateAttrForbiddenSet.add(eventId)
  }

  def isEventAttrBlack(attrId: Integer): Boolean = {
    blackEventAttrIdSet.contains(attrId)
  }

  def isAppUploadData(appId: Integer): Boolean = {
    appIdUploadDataSet.contains(appId)
  }

  def putAppUploadData(appId: Integer): Unit = {
    appIdUploadDataSet.add(appId)
  }

  def putBlackEventName(appId: Integer, owner: String, eventName: String): Unit = {
    blackEventNameSet.add(s"${appId}_${owner}_${eventName}");
  }

  def isEventNameBlack(appId: Integer, owner: String, eventName: String): Boolean = {
    blackEventNameSet.contains(s"${appId}_${owner}_${eventName}");
  }

  def putBlackEventAttrName(appId: Integer, eventId: Integer, owner: String, attrName: String): Unit = {
    blackEventAttrNameSet.add(s"${appId}_${eventId}_${owner}_${attrName}");
  }

  def isEventAttrNameBlack(appId: Integer, eventId: Integer, owner: String, attrName: String): Boolean = {
    blackEventAttrNameSet.contains(s"${appId}_${eventId}_${owner}_${attrName}");
  }

  /**
    * 查询所有的埋点方案开关为1的
    * Map(appid,List[事件])
    */
  def handleOpenAdvertisingFunctionAppId: Unit = {
    val currentOpenAdvertisingFunctionApp = FrontDao.getOpenAdvertisingFunctionAppId()
    val tmpOpenAdvertisingFunctionApp = new ConcurrentHashMap[String, Integer]().asScala
    currentOpenAdvertisingFunctionApp.foreach(f => {
      tmpOpenAdvertisingFunctionApp.put(f._1, f._2)
    })
    openAdvertisingFunctionAppMap = tmpOpenAdvertisingFunctionApp
  }

  def handleLidAndChannelEventMap: Unit = {
    val currentLidAndChannelEvent = FrontDao.getLidAndChannelEvent()
    val tmpLidAndChannelEvent = new ConcurrentHashMap[String, String]().asScala
    currentLidAndChannelEvent.foreach(f => {
      tmpLidAndChannelEvent.put(f._1, f._2)
    })

    lidAndChannelEventMap = tmpLidAndChannelEvent
  }

  def handleAppIdMap: Unit = {
    val appIdSetDB = FrontDao.getEIdMap()
    val tmpLidAndEventIdMap = new ConcurrentHashMap[Integer, Integer]().asScala
    appIdSetDB.foreach(f => {
      tmpLidAndEventIdMap.put(f._1, f._2)
    })
    appIdSMap = tmpLidAndEventIdMap
  }

  def isContainKey(lid: Integer): Boolean = {
    appIdSMap.contains(lid)
  }

  def handleAdFrequency: Unit = {
    val adsFrequencyTempDB = FrontDao.getAdsFrequency()
    val adsFrequencyTemp = Collections.newSetFromMap(new ConcurrentHashMap[String, java.lang.Boolean]()).asScala
    adsFrequencyTempDB.foreach(f => {
      adsFrequencyTemp.add(f)
    })
    adFrequencySet = adsFrequencyTemp
  }

  def putEventLastInsertTime(): Unit = {
    if (eventlastInsertTime.nonEmpty) {
      val currentTimeMillis = System.currentTimeMillis
      if ( ((currentTimeMillis - atomicLong.get()) > 300000)) {
        atomicLong.set(currentTimeMillis)
        FrontDao.putEventLastInsertTime(eventlastInsertTime)
        eventlastInsertTime = new ConcurrentHashMap[Int, String]().asScala
      }
    }
  }

  def handleVirtualEventMap: Unit = {
    val virtualEvent = FrontDao.getVirtualEventMap()
    val tmpVirtualEventMap = new ConcurrentHashMap[String, java.util.ArrayList[String]]().asScala
    virtualEvent.foreach(f => {
      tmpVirtualEventMap.put(f._1, f._2)
    })
    virtualEventMap = tmpVirtualEventMap
  }

  def handleVirtualEventAttrMap: Unit = {
    val virtualEvent = FrontDao.getVirtualEventAttrMap()
    val tmpVirtualEventAttrMap = new ConcurrentHashMap[String, Set[String]]().asScala
    virtualEvent.foreach(f => {
      tmpVirtualEventAttrMap.put(f._1, f._2)
    })
    virtualEventAttrMap = tmpVirtualEventAttrMap
  }

  def handleEventAttrAliasMap: Unit = {
    val virtualEvent = FrontDao.getEventAttrAliasMap()
    val tmpVirtualEventMap = new ConcurrentHashMap[String, String]().asScala
    virtualEvent.foreach(f => {
      tmpVirtualEventMap.put(f._1, f._2)
    })
    eventAttrAliasMap = tmpVirtualEventMap
  }

  def handleVirtualEventAppidsSet: Unit = {
    val virtualEvent = FrontDao.getVirtualEventAppidsSet()
    val tmpVirtualEvent = Collections.newSetFromMap(new ConcurrentHashMap[String, java.lang.Boolean]()).asScala
    virtualEvent.foreach(f => {
      tmpVirtualEvent.add(f)
    })
    virtualEventAppidsSet = tmpVirtualEvent
  }

  def handleAdsLinkEventMap: Unit = {
    val adsLink = FrontDao.getAdsLinkEventMap()
    val tmpAdsLinkEventMap = new ConcurrentHashMap[String, AdsLinkEvent]().asScala
    adsLink.foreach(f => {
      tmpAdsLinkEventMap.put(f._1, f._2)
    })
    adsLinkEventMap = tmpAdsLinkEventMap
  }
}
