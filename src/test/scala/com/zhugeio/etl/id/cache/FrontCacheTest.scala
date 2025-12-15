package com.zhugeio.etl.id.cache

import org.junit._
/**
  * Created by ziwudeng on 10/19/16.
  */
class FrontCacheTest {

  @Test
  def testInit: Unit ={
    val appKey = "80df322ceac8497199bf19801d6db7a3"
    println(FrontCache.getAppId(appKey))
    Thread.sleep(600000)
  }


  @Test
  def testHandleAppKeyAppIdMap: Unit ={
    println(FrontCache.appKeyAppIdMap.mkString(","))
  }

  @Test
  def testHandleAppIdSdkHasDataMap: Unit ={
    println(FrontCache.appIdSdkHasDataMap.mkString(","))
  }

  @Test
  def testHandleAppIdPropIdMap: Unit ={
    println(FrontCache.appIdPropIdMap.mkString(","))
  }

  @Test
  def testHandleAppIdEventIdMap: Unit ={
    println(FrontCache.appIdEventIdMap.mkString(","))
  }

  @Test
  def testHandleAppIdEventAttrIdMap: Unit ={
    println(FrontCache.appIdEventAttrIdMap.mkString(","))
  }

  @Test
  def testHandleAppIdCRreateEventForbidSet: Unit ={
    println(FrontCache.appIdCreateEventForbidSet.mkString(","))
  }

  @Test
  def testHandleBlackEventIdSet: Unit ={
    println(FrontCache.blackEventIdSet.mkString(","))
  }

  @Test
  def testHandleAppIdNoneAutRoCreateSet: Unit ={
    println(FrontCache.appIdNoneAutoCreateSet.mkString(","))
  }

  @Test
  def testHandleBlackEventAttrIdSet: Unit ={
    println(FrontCache.blackEventAttrIdSet.mkString(","))
  }

  @Test
  def testHandleEventIdCreatreAttrForbiddenSet: Unit ={
    println(FrontCache.eventIdCreateAttrForbiddenSet.mkString(","))
  }

  @Test
  def testGetAppId: Unit ={
    val appKey = "80df322ceac8497199bf19801d6db7a3";
    println(FrontCache.getAppId(appKey));
  }

  @Test
  def testPutAppIdKey: Unit ={
    val appKey = "80df322ceac8497199bf19801d6db7a3";
    val appId = 1;

    println(FrontCache.appKeyAppIdMap.mkString(","))
    FrontCache.putAppIdKey(appKey,appId)
    println(FrontCache.appKeyAppIdMap.mkString(","))

  }

  @Test
  def testGetHasData: Unit ={
    val appId = 1;
    val sdk = 1;
    println(FrontCache.getHasData(appId,sdk))
  }

  @Test
  def testPutHasData: Unit ={
    val appId = 1;
    val sdk = 1;
    println(FrontCache.appIdSdkHasDataMap.mkString(","))
    FrontCache.putHasData(appId,sdk)
    println(FrontCache.appIdSdkHasDataMap.mkString(","))
  }

  @Test
  def testGetUserPropId: Unit ={
    val appId = 1;
    val owner = "zg";
    val propName = "gender"
    println(FrontCache.getUserPropId(appId,owner,propName))
  }


}

