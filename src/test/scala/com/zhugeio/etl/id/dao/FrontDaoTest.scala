package com.zhugeio.etl.id.dao

import com.zhugeio.etl.adtoufang.common.Utm
import com.zhugeio.etl.id.cache.FrontCache

import java.sql.PreparedStatement
import com.zhugeio.etl.id.commons.TypeJudger
import org.junit._
import org.springframework.jdbc.core.{BatchPreparedStatementSetter, BeanPropertyRowMapper}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer


/**
  * Created by ziwudeng on 9/14/16.
  */
class FrontDaoTest {

  @Test
  def testGetEventAttr(): Unit = {
    val appId = 1
    val owner = "zg"
    val eventId = 1
    val attrName = "t2"

    println(FrontDao.getEventAttrId(appId, eventId, owner, attrName))
  }


  @Test
  def testInsert(): Unit = {
    val appId = 1
    val owner = "zg"
    val eventId = 1
    val attrName = "平台"
    println(System.getenv("file.encoding"))
    //println(FrontDao.insertEventAttr(appId, eventId, owner, attrName, TypeJudger.getIntType("string")))
  }

  @Test
  def testGetAppIdRight: Unit = {
    val appKey = "80df322ceac8497199bf19801d6db7a3";
    val appId = FrontDao.getAppId(appKey).get
    println(appId)
    Assert.assertEquals(1, appId)
  }

  @Test
  def testGetAppIdWrong: Unit = {
    val appKey = "80df322ceac8497199bf19801d6db722";
    val appId = FrontDao.getAppId(appKey)
    Assert.assertEquals(None, appId)
  }

  @Test
  def testGetAppKeyIdMaps: Unit = {
    val result = FrontDao.getAppKeyIdMaps();
    println(result.mkString(","))
  }

  @Test
  def testGetSdkPlatformHasDataMap: Unit = {
    val result = FrontDao.getSdkPlatformHasDataMap();
    println(result.mkString(","))
  }

  @Test
  def testUpdateHasData: Unit = {
    val appId = 1;
    val sdk = 1;
    val result = FrontDao.getSdkPlatformHasDataMap();
    println(result.mkString(","))

    FrontDao.updateHasData(appId, sdk)

    println(FrontDao.getSdkPlatformHasDataMap().mkString(","))
  }

  @Test
  def testInsertApp: Unit = {
    val appId = 1;
    val sdk = 3;
    val result = FrontDao.getSdkPlatformHasDataMap();
    println(result.mkString(","))

    FrontDao.insertApp(appId, sdk)

    println(FrontDao.getSdkPlatformHasDataMap().mkString(","))
  }

  @Test
  def testGetUserPropId: Unit = {
    val appId = 1;
    val owner = "zg";
    val propName = "gender"

    println(FrontDao.getUserPropId(appId, owner, propName))
  }

  @Test
  def testGetUserPropIds: Unit = {
    println(FrontDao.getUserPropIds().mkString(","))
  }


  @Test
  def testInsertUserProp: Unit = {
    val appId = 1;
    val owner = "zg";
    val propName = "dengziwu20161019"
    println(FrontDao.getUserPropIds().mkString(","))
    FrontDao.insertUserProp(appId, owner, propName, TypeJudger.getIntType("number"))
    println(FrontDao.getUserPropIds().mkString(","))
  }

  @Test
  def testGetEventId: Unit = {
    val appId = 1;
    val owner = "zg";
    val eventName = "购买";
    println(FrontDao.getEventId(appId, owner, eventName))
  }

  @Test
  def testGetEventIds: Unit = {
    println(FrontDao.getEventIds().mkString(","))
  }

  @Test
  def testGetBlackEventIds: Unit = {
    println(FrontDao.getBlackEventIds().mkString(","))
  }

  @Test
  def testInsertEvent: Unit = {
    val appId = 1;
    val owner = "zg";
    val eventName = "购买1";
    println(FrontDao.insertEvent(appId, owner, eventName, "zg_abp"))
    println(FrontDao.getEventIds().mkString(","))
  }

  @Test
  def testGetNoneAutoCreateAppIds: Unit = {
    println(FrontDao.getNoneAutoCreateAppIds())
  }

  @Test
  def testGetForbiddenCreateEventAppIds: Unit = {
    println(FrontDao.getForbiddenCreateEventAppIds())
  }

  @Test
  def testGetEventAttrId: Unit = {
    val appId = 1;
    val eventId = 3
    val owner = "zg";
    val attrName = "数量";

    println(FrontDao.getEventAttrId(appId, eventId, owner, attrName));

  }

  @Test
  def testGetEventAttrIds: Unit = {
    println(FrontDao.getEventAttrIds().mkString(","))
  }

  @Test
  def testGetForbiddenCreateEventAttrEventIdsIds: Unit = {
    println(FrontDao.getForbiddenCreateEventAttrEventIdsIds().mkString(","))
  }

  @Test
  def testGetBlackEventAttrIds: Unit = {
    println(FrontDao.getBlackEventAttrIds().mkString(","))
  }

  @Test
  def testInsertEventAttr: Unit = {
    val appId = 1;
    val eventId = 3
    val owner = "zg";
    val attrName = "数量1";
    println(FrontDao.getEventAttrIds().mkString(","))
    //FrontDao.insertEventAttr(appId, eventId, owner, attrName, TypeJudger.getIntType("number"))
    println(FrontDao.getEventAttrIds().mkString(","))
  }


  @Test
  def insertDate: Unit = {
    val jdbcTemplate = FrontDao.getJdbcTemplate
    val sql = "insert into deng_test(id,date_test) values (?,?)";
    var list = new scala.collection.mutable.ListBuffer[Seq[String]]();
    val list1 = new scala.collection.mutable.ListBuffer[String]
    list1 += "1"
    list1 += "2017-02-01 23:11:00"

    val list2 = new scala.collection.mutable.ListBuffer[String]
    list2 += "2"
    list2 += "2017-02-02 23:11:00"

    list += list1;
    list += list2;

    jdbcTemplate.batchUpdate(sql, new BatchPreparedStatementSetter {
      override def setValues(ps: PreparedStatement, i: Int): Unit = {
        ps.setObject(1, list(i)(0))
        ps.setObject(2, list(i)(1))
      }

      override def getBatchSize: Int = {
        list.size
      }
    })

  }

  @Test
  def select: Unit = {
    //    val str = FrontDao.getManagementContril()
    //    val user: mutable.HashMap[Integer, java.util.List[String]] = mutable.HashMap()
    //    for (s <- 0 until str.size()) {
    //      val appId: Integer = str.get(s)
    //      val strings: util.List[String] = FrontDao.getMangementEventPlanByappid(appId)
    //      user += (appId -> strings)
    //      println(strings)
    //    }
    //    println(user)
    val s = "_wowowowo"
    println(s.substring(1))
    println(s)
  }

  @Test
  def testGetAttrCount(): Unit = {
    //FrontCache.handleAdFrequency
    println(FrontCache.adFrequencySet)
    // println(FrontDao.getAdsFrequency())
  }

  @Test
  def testAdApp(): Unit = {
    //println(FrontDao.isAdApp("bffbb35c387d4422bb53978bf504991f"))
    println(FrontDao.getConvent(3272, 174).toJsonString())
    println(FrontDao.getUTM(174).toJsonString)

    val labelRowMapper = BeanPropertyRowMapper.newInstance(classOf[Utm])
    val list = FrontDao.getJdbcTemplate.queryForList("select * from ads_link_toufang where id = ? ", 174 + "")
    println(list.toString)
    println(FrontDao.getFrequency(3272, 174))
  }

  @Test
  def testImpala(): Unit = {
    val list = ImpalaDao.querySQLForOne("select property_value from b_user_property_177 where property_id= 651 limit 1", "property_value")
    println(list.toString)
  }

  @Test
  def putEventLastInsertTime(): Unit = {
    val eventlastInsertTime: mutable.HashMap[Int, String] = mutable.HashMap()
    eventlastInsertTime += (1 -> "2022-11-08 13:57:17")
    eventlastInsertTime += (2 -> "2022-11-08 13:57:17")
    eventlastInsertTime += (3 -> "2022-11-08 13:57:17")
    eventlastInsertTime += (4 -> "2022-11-08 13:57:17")
    eventlastInsertTime += (5 -> "2022-11-08 13:57:17")
    eventlastInsertTime.foreach(x => {
      val sql = "UPDATE `event` SET `last_insert_time` = '" + x._2 + "' WHERE `id` = " + x._1
      FrontDao.getJdbcTemplate.update(sql)
    })
  }

}
