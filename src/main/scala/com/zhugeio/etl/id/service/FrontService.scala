package com.zhugeio.etl.id.service

import com.zhugeio.etl.id.cache.FrontCache
import com.zhugeio.etl.id.dao.FrontDao

/**
  * Created by ziwudeng on 9/13/16.
  */
object FrontService {

  /**
    * 获得应用ID
    *
    * @param appKey
    * @return
    */
  def getAppId(appKey: String): Option[Integer] = {
    val cacheAppId = FrontCache.getAppId(appKey)
    //      if(cacheAppId.isEmpty){
    //        val dbAppId = FrontDao.getAppId(appKey);
    //        if(dbAppId.isEmpty){
    //          return dbAppId;
    //        }else{
    //          FrontCache.putAppIdKey(appKey,dbAppId.get)
    //          return dbAppId
    //        }
    //      }else{
    //        return cacheAppId
    //      }
    cacheAppId
  }


  /**
    * 数据库里插入app_platform
    *
    * @param appId
    * @param platform
    */
  def handleAppPlat(appId: Integer, platform: Integer) = {
    val cacheHasData = FrontCache.getHasData(appId, platform)
    if (cacheHasData.isEmpty) {
      FrontDao.insertAppCreateNotice(appId)
      FrontDao.insertApp(appId, platform)
      FrontDao.updateHasData(appId, platform)
      FrontCache.putHasData(appId, platform)
    } else {
      if (cacheHasData.get == 0) {
        FrontDao.insertAppCreateNotice(appId)
        FrontDao.updateHasData(appId, platform)
        FrontCache.putHasData(appId, platform)
      } else {
        ;
      }
    }
  }

  def handleAppUpload(appId: Integer, platform: Integer): Unit = {
    val hasUpload = FrontCache.isAppUploadData(appId)
    if (!hasUpload) {
      FrontDao.insertUploadData(appId)
      FrontCache.putAppUploadData(appId)
    }
  }

  /**
    * 获得用户属性ID
    *
    * @param appId
    * @param owner
    * @param prop
    * @return
    */
  def getUserPropId(appId: Integer, owner: String, prop: String, typeInt: Integer): Option[Integer] = {

    var userPropIdOption = FrontCache.getUserPropId(appId, owner, prop)

    if (userPropIdOption.isEmpty) {
      userPropIdOption = FrontDao.getUserPropId(appId, owner, prop)
    }
    if (userPropIdOption.isDefined) {
      if (FrontCache.isUserPropBlack(userPropIdOption.get)) {
        println(s"userProp is black,prop:${prop},userPropId:"+userPropIdOption.get)
        return None
      }
    }
    if (userPropIdOption.isEmpty) {
      FrontDao.insertUserProp(appId, owner, prop, typeInt)
      val userPropIdDBNew = FrontDao.getUserPropId(appId, owner, prop)
      if (userPropIdDBNew.isDefined) {
        if (FrontCache.isUserPropBlack(userPropIdDBNew.get)) {
          return None
        }
        userPropIdOption = userPropIdDBNew
      }
    }

    if (userPropIdOption.isDefined) {
      FrontCache.putUserPropId(appId, owner, prop, userPropIdOption.get)
    }
    userPropIdOption
  }

  /**
    * 获得事件ID
    *
    * @param appId
    * @param owner
    * @param eventName
    * @return
    */
  def getEventId(appId: Integer, owner: String, eventName: String, aliasName: String): Option[Integer] = {
    if (FrontCache.isEventNameBlack(appId, owner, eventName)) {
      return None
    }
    if (eventName.indexOf("\uD83D\uDC16") > 0) {
      return None
    }
    var eventIdOption = FrontCache.getEventId(appId, owner, eventName)
    if (eventIdOption.isEmpty) {
      eventIdOption = FrontDao.getEventId(appId, owner, eventName)
    }
    if (eventIdOption.isDefined) {
      if (FrontCache.isEventBlack(eventIdOption.get)) {
        println(s"event is black,eventName:${eventName},eventId:"+eventIdOption.get)
        return None
      }
    }
    var finalEventIdOption = eventIdOption
    if (eventIdOption.isEmpty) {
      if (FrontCache.isAppIdCreateEventForbid(appId) ||
        // 开启埋点控制的应用要放行虚拟事件
        (!FrontCache.isAppIdAutoCreate(appId) && !owner.equals("zg_vtl"))) {
        return None
      }
      if (FrontDao.isEventCreateExceed(appId)) {
        FrontCache.putAppIdCreateEventForbid(appId)
        return None
      }
      //TODO 将新的事件id插入事件表
      FrontDao.insertEvent(appId, owner, eventName, aliasName)
      val eventIdOptionDBNew = FrontDao.getEventId(appId, owner, eventName)
      if (eventIdOptionDBNew.isEmpty) {
        FrontCache.putBlackEventName(appId, owner, eventName)
        return None
      } else {
        if (FrontCache.isEventBlack(eventIdOptionDBNew.get)) {
          return None
        }
        finalEventIdOption = eventIdOptionDBNew
        FrontCache.putEventIdMap(appId, owner, eventName, eventIdOptionDBNew.get)
      }
    }
    finalEventIdOption
  }

  def handleEventPlatform(appId: Integer, eventId: Integer, sdk: Integer): Unit = {
    if (!FrontCache.hasEventPlat(appId, eventId, sdk)) {
      FrontDao.insertEventPlatform(appId, eventId, sdk)
      FrontCache.putEventPlat(appId, eventId, sdk)
    }
  }


  /**
    * 获得设备属性ID
    *
    * @param appId
    * @param owner
    * @param propName
    * @return
    */
  def getDevicePropId(appId: Integer, owner: String, propName: String): Option[Integer] = {
    val devicePropIdOption = FrontCache.getDevicePropId(appId, owner, propName)
    if (devicePropIdOption.isEmpty) {
      val devicePropIdOptionDB = FrontDao.getDevicePropId(appId, owner, propName)
      if (devicePropIdOptionDB.isEmpty) {
        FrontDao.insertDeviceProp(appId, owner, propName)

        val devicePropIdOptionDBNew = FrontDao.getDevicePropId(appId, owner, propName)
        if (devicePropIdOptionDBNew.isEmpty) {
          throw new IllegalStateException(s"deviceProp no reason:${appId}_${owner}_${propName}")
        } else {
          FrontCache.putDevicePropIdMap(appId, owner, propName, devicePropIdOptionDBNew.get)
          devicePropIdOptionDBNew
        }
      } else {
        devicePropIdOptionDB
      }
    } else {
      devicePropIdOption
    }
  }

  /**
    * 设备属性ID的平台
    *
    * @param appId
    * @param propId
    * @param sdk
    */
  def handleDevicePropPlat(appId: Integer, propId: Integer, sdk: Integer): Unit = {
    if (!FrontCache.hasDevicePropPlat(appId, propId, sdk)) {
      FrontDao.insertDevicePropPlatform(appId, propId, sdk);
      FrontCache.putDevicepropPlat(appId, propId, sdk)
    }
  }

  def canAddEvent(appId: Integer): Boolean = {
    if (!FrontCache.isAppIdAutoCreate(appId)) {
      return false;
    } else {
      if (FrontCache.isAppIdCreateEventForbid(appId)) {
        return false;
      } else {
        return true;
      }
    }
  }

  /**
    * 获得事件属性ID
    *
    * @param appId
    * @param owner
    * @param eventId
    * @param attrName
    * @return
    */
  def getEventAttrId(appId: Integer, eventId: Integer, owner: String, attrName: String,attrAliasName: String, attrIntType: Integer): Option[Integer] = {
    //每个事件都会进行循环
    if (FrontCache.isEventAttrNameBlack(appId, eventId, owner, attrName)) {
      return None
    }
    if (attrName.indexOf("\uD849\uDCB0") > 0 || attrName.indexOf("\uD85C\uDEE6") > 0  || attrName.indexOf("\uD84F\uDCC7") > 0  || attrName.indexOf("\uD840\uDE5B") > 0) {
      return None
    }
    var eventAttrIdOption = FrontCache.getEventAttrId(appId, eventId, owner, attrName)
    if (eventAttrIdOption.isEmpty) {
      eventAttrIdOption = FrontDao.getEventAttrId(appId, eventId, owner, attrName)
    }
    if (eventAttrIdOption.isDefined) {
      if (FrontCache.isEventAttrBlack(eventAttrIdOption.get)) {
        println(s"eventAttr is black:attrName:${attrName},eventAttrId:"+eventAttrIdOption.get)
        return None
      }
    }
    var finalEventAttrIdOption = eventAttrIdOption
    if (eventAttrIdOption.isEmpty) {
      if ((!FrontCache.isAppIdAutoCreate(appId) && !owner.equals("zg_vtl")) || FrontCache.isEventIdCreateEventAttrForbid(eventId)) {
        return None
      }

      if (FrontDao.isEventAttrExceed(appId, eventId)) {
        FrontCache.putEventIdCreateEventAttrForbid(eventId)
        return None
      } else {
        var columnName = "cus"
        var v = FrontDao.getAttrCount(appId, eventId, owner)
        if (null == v || v.isEmpty || v.equals("NULL")) {
          columnName = "cus1"
        } else {
          val columnNameId = v.toInt + 1
          columnName = columnName + columnNameId
        }

        FrontDao.insertEventAttr(appId, eventId, owner, attrName,attrAliasName, attrIntType, columnName)
      }

      val eventIdOptionDBNew = FrontDao.getEventAttrId(appId, eventId, owner, attrName)
      if (eventIdOptionDBNew.isEmpty) {
        FrontCache.putBlackEventAttrName(appId, eventId, owner, attrName);
        return None;
      } else {
        if (FrontCache.isEventAttrBlack(eventIdOptionDBNew.get)) {
          return None
        }
        finalEventAttrIdOption = eventIdOptionDBNew
        FrontCache.putEventAttrId(appId, eventId, owner, attrName, eventIdOptionDBNew.get)
      }
    } else {
      finalEventAttrIdOption = eventAttrIdOption
    }
    finalEventAttrIdOption


  }

  def handleEventAttrPlat(appId: Integer, eventId: Integer, attrId: Integer, sdk: Integer): Unit = {
    if (!FrontCache.hasEventAttrPlat(appId, eventId, attrId, sdk)) {
      FrontDao.insertEventAttrPlatform(appId, eventId, attrId, sdk)
      FrontCache.putEventAttrPlat(appId, eventId, attrId, sdk)
    }
  }


  def main(args: Array[String]): Unit = {

  }
}
