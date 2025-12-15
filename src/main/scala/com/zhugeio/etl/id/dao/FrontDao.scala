package com.zhugeio.etl.id.dao

import com.alibaba.fastjson.{JSON, JSONObject}
import com.zhugeio.etl.adtoufang.common.Utm
import com.zhugeio.etl.id.adtoufang.common.AdsLinkEvent
import com.zhugeio.etl.id.cache.FrontCache
import com.zhugeio.etl.id.db.Datasource
import com.zhugeio.etl.id.log.IdLogger
import org.springframework.jdbc.core.BeanPropertyRowMapper
import org.springframework.jdbc.core.support.JdbcDaoSupport

import java.util
import scala.collection.convert.decorateAsScala._
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
  * Created by ziwudeng on 9/12/16.
  */
object FrontDao extends JdbcDaoSupport {

  this.setDataSource(Datasource.front)

  /**
    * 获得应用ID
    *
    * @param appKey
    * @return
    */
  def getAppId(appKey: String): Option[Integer] = {
    val sql = "select id from company_app where app_key = ? and is_delete = 0 and stop = 0 and id not in (select id from tmp_transfer where status = 2)"
    val list = this.getJdbcTemplate.queryForList(sql, appKey);
    if (list.isEmpty) {
      return None;
    } else {
      return Some(Option(list.get(0).get("id")).map(_.toString.toInt.asInstanceOf[Integer]).orNull)
    }
  }

  /**
    * 获得<appKey,id> map
    *
    * @return Map
    */
  def getAppKeyIdMaps(): Map[String, Integer] = {
    val sql = "select app_key,id from company_app where is_delete = 0 and stop = 0 and id not in (select id from tmp_transfer where status = 2) "
    val list = this.getJdbcTemplate.queryForList(sql)
    var mapResult = Map[String, Integer]()

    val iterator = list.iterator()
    while (iterator.hasNext) {
      val row = iterator.next()
      mapResult = mapResult.+(row.get("app_key").asInstanceOf[String] -> Option(row.get("id")).map(_.toString.toInt.asInstanceOf[Integer]).orNull);
    }
    return mapResult
  }

  /**
   * 更新事件属性类型
   * @param attr_id
   * @param attrIntType
   * @return
   */
  def upsertEventAttrIntType(attr_id: Integer, attrIntType: Integer) = {
    val sql = "update event_attr set prop_type = ? where attr_id = ?"
    this.getJdbcTemplate.update(sql, attrIntType, attr_id)
  }

  def getEventVirtualAttrIds():mutable.Set[String] = {
    val sql =
      """
        |select ea.attr_id
        |from event_attr ea
        |where ea.is_delete = 0
        |  and ea.attr_type = 1
        |group by ea.attr_id
        |""".stripMargin
    val list = this.getJdbcTemplate.queryForList(sql)
    val result = new mutable.HashSet[String]()
    val iterator = list.iterator()
    while (iterator.hasNext) {
      val row = iterator.next()
      val attr_id = row.get("attr_id").asInstanceOf[Long];
      result.add(attr_id.toString)
    }
    result
  }

  /**
    * 获得<appId_platform,hasData> map
    *
    * @return
    */
  def getSdkPlatformHasDataMap(): Map[String, Integer] = {
    val sql = "select main_id,sdk_platform,has_data from app"
    val list = this.getJdbcTemplate.queryForList(sql)
    var mapResult = Map[String, Integer]()

    val iterator = list.iterator()
    while (iterator.hasNext) {
      val row = iterator.next()
      val mainId = Option(row.get("main_id")).map(_.toString.toInt.asInstanceOf[Integer]).orNull;
      val sdk = Option(row.get("sdk_platform")).map(_.toString.toInt.asInstanceOf[Integer]).orNull;
      val hasData = Option(row.get("has_data")).map(_.toString.toInt.asInstanceOf[Integer]).orNull;

      mapResult = mapResult.+(s"${mainId}_${sdk}" -> hasData);

    }
    return mapResult
  }

  /**
    * 更新app表示有数据
    *
    * @param appId
    * @param platform
    */
  def updateHasData(appId: Integer, platform: Integer): Unit = {
    val sql = "update app set has_data = 1,has_data_time=now() where main_id = ? and sdk_platform = ? ";
    this.getJdbcTemplate.update(sql, appId, platform);
  }

  /**
    * 插入app
    *
    * @param appId
    * @param platform
    */
  def insertApp(appId: Integer, platform: Integer): Unit = {
    val sql = "insert ignore into app(main_id,sdk_platform,config_info,has_data,creat_time,is_delete,has_data_time) values (?,?,null,1,now(),0,now())"
    this.getJdbcTemplate.update(sql, appId, platform);
  }

  /**
    * 插入创建APP通知表
    *
    * @param appId
    */
  def insertAppCreateNotice(appId: Integer): Unit = {
    val sql = "insert ignore into app_create_notice(app_id,created,insert_time) values (?,0,now())"
    this.getJdbcTemplate.update(sql, appId)
  }

  /**
    * 插入创建事件通知表
    *
    * @param appId
    * @param eventId
    */
  def insertEventCreateNotice(appId: Integer, eventId: Integer): Unit = {
    val sql = "insert ignore into event_create_notice(app_id,event_id,created,insert_time) values(?,?,0,now())"
    this.getJdbcTemplate.update(sql, appId, eventId)
  }

  /**
    * 插入应用开始上传数据记录
    *
    * @param appId
    */
  def insertUploadData(appId: Integer): Unit = {
    val sql = "insert ignore into app_data(app_id,first) values (?,now())"
    this.getJdbcTemplate.update(sql, appId);
  }

  /**
    * 获得应用开始上传数据记录
    */
  def getUploadDatas(): mutable.Set[Integer] = {
    val sql = "select app_id from app_data"
    return new java.util.HashSet(this.getJdbcTemplate.queryForList(sql, classOf[Integer])).asScala
  }

  /**
    * 获得用户属性ID
    *
    * @param appId
    * @param owner
    * @param propName
    * @return
    */
  def getUserPropId(appId: Integer, owner: String, propName: String): Option[Integer] = {
    val sql = "select id from user_prop_meta where app_id = ? and owner = ? and name = ? "
    val list = this.getJdbcTemplate.queryForList(sql, appId, owner, propName);
    if (list.isEmpty) {
      return None
    } else {
      return Some(Option(list.get(0).get("id")).map(_.toString.toInt.asInstanceOf[Integer]).orNull)
    }
  }

  def getUserPropIds(): Map[String, Integer] = {
    val sql = "select app_id,owner,name,id from user_prop_meta"
    val list = this.getJdbcTemplate.queryForList(sql)
    var mapResult = Map[String, Integer]()
    val iterator = list.iterator()
    while (iterator.hasNext) {
      val row = iterator.next()
      val appId = Option(row.get("app_id")).map(_.toString.toInt.asInstanceOf[Integer]).orNull;
      val ownerEscape = row.get("owner").asInstanceOf[String];
      val name = row.get("name").asInstanceOf[String].toUpperCase;
      val id = Option(row.get("id")).map(_.toString.toInt.asInstanceOf[Integer]).orNull;
      mapResult = mapResult.+(s"${appId}_${ownerEscape}_${name}" -> id);
    }
    return mapResult
  }

  def getOriginalUserPropIds(): Map[String, String] = {
    val sql = "select app_id,owner,name,id from user_prop_meta"
    val list = this.getJdbcTemplate.queryForList(sql)
    var mapResult = Map[String, String]()
    val iterator = list.iterator()
    while (iterator.hasNext) {
      val row = iterator.next()
      val appId = Option(row.get("app_id")).map(_.toString.toInt.asInstanceOf[Integer]).orNull;
      val ownerEscape = row.get("owner").asInstanceOf[String];
      val name = row.get("name").asInstanceOf[String]
      val id = Option(row.get("id")).map(_.toString.toInt.asInstanceOf[Integer]).orNull;
      mapResult = mapResult.+(s"${appId}_${ownerEscape}_${id}" -> name);
    }
    return mapResult
  }

  def getBlackUserPropIds(): mutable.Set[Integer] = {
    val sql = "select id from user_prop_meta where is_delete = 1 "
    new java.util.HashSet(this.getJdbcTemplate.queryForList(sql, classOf[Integer])).asScala
  }


  /**
    * 插入用户属性ID
    *
    * @param appId
    * @param owner
    * @param propName
    * @return
    */
  def insertUserProp(appId: Integer, owner: String, propName: String, propType: Integer): Integer = {
    val sql = "insert ignore into user_prop_meta(app_id,owner,name,type) values(?,?,?,?) "
    this.getJdbcTemplate.update(sql, appId, owner, propName, propType)
  }

  /**
    * 获得事件ID
    *
    * @param appId
    * @param owner
    * @param eventName
    * @return
    */
  def getEventId(appId: Integer, owner: String, eventName: String): Option[Integer] = {
    try {
      val sql = "select id from event where app_id = ? and owner = ? and event_name = ? "
      val list = this.getJdbcTemplate.queryForList(sql, appId, owner, eventName);
      if (list.isEmpty) {
        return None
      } else {
        return Some(Option(list.get(0).get("id")).map(_.toString.toInt.asInstanceOf[Integer]).orNull)
      }
    }catch {
      case e:Exception=>{
        e.printStackTrace()
        logger.error("getEventId error",e)
        return None
      }
    }
  }

  def getEventIds(): Map[String, Integer] = {
    val sql = "select app_id,owner,event_name,id from event"
    val list = this.getJdbcTemplate.queryForList(sql)
    var mapResult = Map[String, Integer]()

    val iterator = list.iterator()
    while (iterator.hasNext) {
      val row = iterator.next()
      val appId = Option(row.get("app_id")).map(_.toString.toInt.asInstanceOf[Integer]).orNull.toInt;
      val ownerEscape = row.get("owner").asInstanceOf[String];
      val name = row.get("event_name").asInstanceOf[String];
      val id = Option(row.get("id")).map(_.toString.toInt.asInstanceOf[Integer]).orNull.toInt;
      mapResult = mapResult.+(s"${appId}_${ownerEscape}_${name}" -> id);
    }
    return mapResult

  }

  def getBlackEventIds(): mutable.Set[Integer] = {
    val sql = "select id from event where is_delete = 1 or is_stop = 1 "
    new java.util.HashSet(this.getJdbcTemplate.queryForList(sql, classOf[Integer])).asScala
  }

  /**
    * 插入事件表
    *
    * @param appId
    * @param owner
    * @param eventName
    */
  def insertEvent(appId: Integer, owner: String, eventName: String, aliasName: String): Unit = {
    val sql = "insert ignore into event(app_id,owner,event_name,insert_time,alias_name) values(?,?,?,now(),?) "
    println("insertEvent sql:" + sql)
    try {
      this.getJdbcTemplate.update(sql, appId, owner, eventName, aliasName)
    } catch {
      case e: Exception => {
        e.getStackTrace
      }
    }
  }

  /**
    * 插入事件平台表
    *
    * @param appId
    * @param eventId
    * @param sdk
    */
  def insertEventPlatform(appId: Integer, eventId: Integer, sdk: Integer): Unit = {
    val sql = "insert ignore into event_platform(event_id, platform) values(?, ?)"
    IdLogger.info(s"${sql},${eventId},${sdk}");
    this.getJdbcTemplate.update(sql, eventId, sdk)
  }

  /**
    * 获得事件平台
    *
    * @return
    */
  def getEventPlatforms(): mutable.Set[String] = {
    val sql = "select event_id,platform from event_platform where event_id in (select id from event where is_delete = 0 and is_stop = 0 and app_id in (select id from company_app where is_delete = 0) ) "
    val list = this.getJdbcTemplate.queryForList(sql);
    val iterator = list.iterator()
    val result = new mutable.HashSet[String]()
    while (iterator.hasNext) {
      val row = iterator.next();
      val eventId = String.valueOf(row.get("event_id"));
      val platform = String.valueOf(row.get("platform"))
      result.add(s"${eventId}_${platform}")
    }
    result
  }

  /**
    * 插入设备属性表
    *
    * @param appId
    * @param owner
    * @param propName
    */
  def insertDeviceProp(appId: Integer, owner: String, propName: String): Unit = {
    val sql = "insert ignore into device_prop(app_id,owner,name) values(?,?,?) "
    this.getJdbcTemplate.update(sql, appId, owner, propName)
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
    val sql = "select id from device_prop where app_id = ? and owner = ? and name = ? "
    val list = this.getJdbcTemplate.queryForList(sql, appId, owner, propName);
    if (list.isEmpty) {
      return None
    } else {
      return Some(Option(list.get(0).get("id")).map(_.toString.toInt.asInstanceOf[Integer]).orNull)
    }
  }

  /**
    * 获得设备属性ID
    *
    * @return
    */
  def getDevicePropIds(): Map[String, Integer] = {
    val sql = "select app_id,owner,name,id from device_prop"
    val list = this.getJdbcTemplate.queryForList(sql)
    var mapResult = Map[String, Integer]()

    val iterator = list.iterator()
    while (iterator.hasNext) {
      val row = iterator.next()
      val appId = Option(row.get("app_id")).map(_.toString.toInt.asInstanceOf[Integer]).orNull.toInt;
      val ownerEscape = row.get("owner").asInstanceOf[String];
      val name = row.get("event_name").asInstanceOf[String];
      val id = Option(row.get("id")).map(_.toString.toInt.asInstanceOf[Integer]).orNull.toInt;
      mapResult = mapResult.+(s"${appId}_${ownerEscape}_${name}" -> id);
    }
    return mapResult
  }

  /**
    * 插入设备属性平台
    *
    * @param appId
    * @param propId
    * @param sdk
    */
  def insertDevicePropPlatform(appId: Integer, propId: Integer, sdk: Integer): Unit = {
    val sql = "insert ignore into device_prop_platform(prop_id, platform) values(?, ?)"
    this.getJdbcTemplate.update(sql, propId, sdk)
  }

  /**
    * 获得设备属性平台
    *
    * @return
    */
  def getDevicePropPlatforms(): mutable.Set[String] = {
    val sql = "select prop_id,platform from device_prop_platform "
    val list = this.getJdbcTemplate.queryForList(sql);
    val iterator = list.iterator()
    val result = new mutable.HashSet[String]()
    while (iterator.hasNext) {
      val row = iterator.next();
      val prop_id = String.valueOf(row.get("prop_id"));
      val platform = String.valueOf(row.get("platform"))
      result.add(s"${prop_id}_${platform}")
    }
    result
  }


  /**
    * 插入事件属性表平台
    *
    * @param appId
    * @param eventId
    * @param attrId
    * @param sdk
    */
  def insertEventAttrPlatform(appId: Integer, eventId: Integer, attrId: Integer, sdk: Integer): Unit = {
    val sql = "insert ignore into event_attr_platform(event_attr_id, platform) values(?, ?)"
    IdLogger.info(s"${sql},${eventId},${attrId},${sdk}");
    this.getJdbcTemplate.update(sql, attrId, sdk)
  }


  /**
    * 获得属性平台
    *
    * @return
    */
  def getEventAttrPlatforms(): mutable.Set[String] = {
    val sql = "select a.event_attr_id,a.platform from event_attr_platform  a join event_attr k on a.event_attr_id = k.attr_id join event b on k.event_id = b.id join company_app f on f.id = b.app_id where f.is_delete = 0 and b.is_delete = 0 "
    val list = this.getJdbcTemplate.queryForList(sql);
    val iterator = list.iterator()
    val result = new mutable.HashSet[String]()
    while (iterator.hasNext) {
      val row = iterator.next();
      val eventAttrId = String.valueOf(row.get("event_attr_id"));
      val platform = String.valueOf(row.get("platform"))
      result.add(s"${eventAttrId}_${platform}")
    }
    result
  }


  def getNoneAutoCreateAppIds(): mutable.Set[Integer] = {
    val sql = "select id from company_app where is_delete = 0 and auto_event = 0 "
    return new java.util.HashSet(this.getJdbcTemplate.queryForList(sql, classOf[Integer])).asScala
  }

  def getForbiddenCreateEventAppIds(): mutable.Set[Integer] = {
    val sql = "select a.id from company_app a,event b where a.is_delete = 0 and b.is_delete = 0 and b.owner = 'zg' and b.is_stop = 0 and a.id = b.app_id group by a.id having count(*) >= max(a.event_sum) "
    return new java.util.HashSet(this.getJdbcTemplate.queryForList(sql, classOf[Integer])).asScala
  }

  /**
    * 获得事件属性ID
    *
    * @param appId
    * @param eventId
    * @param owner
    * @return
    */
  def getEventAttrId(appId: Integer, eventId: Integer, owner: String, attrName: String): Option[Integer] = {
    try {
      val sql = "select attr_id from event_attr where owner = ? and event_id = ? and attr_name = ? "
      IdLogger.info(s"${sql},${eventId},${owner},${attrName}");
      val list = this.getJdbcTemplate.queryForList(sql, owner, eventId, attrName);
      if (list.isEmpty) {
        return None
      } else {
        return Some(Option(list.get(0).get("attr_id")).map(_.toString.toInt.asInstanceOf[Integer]).orNull)
      }
    }catch {
      case e:Exception=>{
        IdLogger.error(s"${e.getMessage}",e)
        return None
      }
    }
  }

  def getEventAttrIds(): Map[String, Integer] = {
    val sql = "select b.app_id,a.event_id,a.attr_id,a.attr_name,a.owner from event_attr a join event b on a.event_id = b.id join company_app f on f.id = b.app_id where f.is_delete = 0"
    val list = this.getJdbcTemplate.queryForList(sql)
    var mapResult = Map[String, Integer]()

    val iterator = list.iterator()
    while (iterator.hasNext) {
      val row = iterator.next()
      val appId = Option(row.get("app_id")).map(_.toString.toLong).getOrElse(0L).toInt;
      val ownerEscape = row.get("owner").asInstanceOf[String];
      val eventId = row.get("event_id").asInstanceOf[Long].toInt;
      val name = row.get("attr_name").asInstanceOf[String].toUpperCase;
      val id = row.get("attr_id").asInstanceOf[Long].toInt;
      mapResult = mapResult.+(s"${appId}_${eventId}_${ownerEscape}_${name}" -> id);
    }
    return mapResult
  }


  def getForbiddenCreateEventAttrEventIdsIds(): mutable.Set[Integer] = {
    val sql = "select b.id from company_app a,event b,event_attr c where a.is_delete = 0 and b.is_delete = 0 and b.is_stop = 0 and a.id = b.app_id  and b.id = c.event_id and c.is_stop = 0 group by b.id having count(*) >= max(a.attr_sum)"
    return new java.util.HashSet(this.getJdbcTemplate.queryForList(sql, classOf[Integer])).asScala
  }

  def getBlackEventAttrIds(): mutable.Set[Integer] = {
    val sql = "select c.attr_id from event_attr c where (c.is_delete = 1 or  c.is_stop = 1) and c.attr_type != 1 "
    return new java.util.HashSet(this.getJdbcTemplate.queryForList(sql, classOf[Integer])).asScala
  }

  def getVirtualUserPropMap() = {
    val sql = "select app_id, name, sql_json, table_fields from user_prop_meta where is_delete = 0 and attr_type = 1"
    val list = this.getJdbcTemplate.queryForList(sql)
    // 修改键类型为 String
    var mapResult = Map[String, java.util.ArrayList[String]]()

    val iterator = list.iterator()
    while (iterator.hasNext) {
      val row = iterator.next()
      val attrName = row.get("name").asInstanceOf[String]
      val appId = row.get("app_id") match {
        case i: java.lang.Integer => i.toLong
        case l: java.lang.Long => l
        case _ => throw new ClassCastException("app_id is not a valid numeric type")
      }
      val sqlJson = row.get("sql_json").asInstanceOf[String]
      val tableFields = row.get("table_fields").asInstanceOf[String]

      // 创建符合要求格式的JSON对象
      val jsonObj = new JSONObject()
      jsonObj.put("name", attrName)
      jsonObj.put("define", sqlJson)
      jsonObj.put("tableFields", tableFields)

      // 将键转换为字符串类型
      val key = appId.toString // 使用 appId 的字符串形式

      var optionValue = mapResult.get(key)
      if (optionValue.isDefined) {
        val arrJson = optionValue.get
        arrJson.add(jsonObj.toJSONString)
        mapResult = mapResult.+(key -> arrJson)
      } else {
        val arrJson = new java.util.ArrayList[String]()
        arrJson.add(jsonObj.toJSONString)
        mapResult = mapResult.+(key -> arrJson)
      }
    }
    mapResult
  }


  /**
   * 获取虚拟属性
   */
  def getVirtualEventPropMap() = {
    val sql = "select e.app_id,ea.attr_name,e.id as event_id,e.event_name,ea.sql_json from event_attr ea left join event e on e.id = ea.event_id where ea.is_delete=0 and ea.attr_type=1"
    val list = this.getJdbcTemplate.queryForList(sql)
    var mapResult = Map[String, java.util.ArrayList[String]]()

    val iterator = list.iterator()
    while (iterator.hasNext) {
      val row = iterator.next()
      val attrName = row.get("attr_name").asInstanceOf[String]
      val appId = row.get("app_id").asInstanceOf[Long]
      val eventName = row.get("event_name").asInstanceOf[String]
      val sqlJson = row.get("sql_json").asInstanceOf[String]

      // 创建符合要求格式的JSON对象
      val jsonObj = new JSONObject()
      jsonObj.put("name", attrName)
      jsonObj.put("define", sqlJson)

      // 构造新的key格式: ${appId}_eP_$eventName
      val key = s"${appId}_eP_${eventName}"

      var optionValue = mapResult.get(key)
      if (optionValue.isDefined) {
        val arrJson = optionValue.get
        arrJson.add(jsonObj.toJSONString)
        mapResult = mapResult.+(key -> arrJson)
      } else {
        val arrJson = new java.util.ArrayList[String]()
        arrJson.add(jsonObj.toJSONString)
        mapResult = mapResult.+(key -> arrJson)
      }
    }
    mapResult
  }

  /**
   * 获取虚拟属性应用id
   */
  def getVirtualPropAppIdsSet: mutable.Set[String] = {
    val sql =
      """
        |SELECT e.app_id
        |FROM event_attr ea
        |LEFT JOIN event e ON ea.event_id = e.id
        |WHERE ea.is_delete = 0
        |  AND ea.attr_type = 1
        |GROUP BY e.app_id
        |
        |UNION
        |
        |SELECT app_id
        |FROM user_prop_meta
        |WHERE is_delete = 0
        |  AND attr_type = 1
        |""".stripMargin
    val list = this.getJdbcTemplate.queryForList(sql)
    val result = new mutable.HashSet[String]()
    val iterator = list.iterator()
    while (iterator.hasNext) {
      val row = iterator.next()
      val appId = Option(row.get("app_id")).map(_.toString.toLong).getOrElse(0L);
      result.add(appId.toString)
    }
    result
  }

  /**
    * 插入事件表
    *
    * @param appId
    * @param eventId
    * @param attrName
    *
    */
  def insertEventAttr(appId: Integer, eventId: Integer, owner: String, attrName: String, attrAliasName: String, attrIntType: Integer, columnName: String): Unit = {
    val sql = "insert ignore into event_attr(owner,event_id,attr_name,alias_name,prop_type,column_name,insert_time) values(?,?,?,?,?,?,now()) "
    IdLogger.info(s"${sql},${eventId},${owner},${attrName}");
    this.getJdbcTemplate.update(sql, owner, eventId, attrName, attrAliasName, attrIntType, columnName)
  }

  def isEventAttrExceed(appId: Integer, eventId: Integer): Boolean = {
    val sql = "SELECT a.attr_sum - b.currentSum FROM company_app a,   (SELECT count(*) AS currentSum    FROM event_attr    WHERE event_id = ? and is_stop = 0) b WHERE a.id = ? ;"
    val diff = this.getJdbcTemplate.queryForObject(sql, classOf[Integer], eventId, appId);
    diff <= 0;
  }

  def isEventCreateExceed(appId: Integer): Boolean = {
    val sql = "SELECT a.event_sum - b.currentSum FROM company_app a,(SELECT count(*) AS currentSum    FROM event    WHERE app_id = ?      AND is_stop = 0 and is_delete = 0 and owner = 'zg' ) b WHERE a.id = ?";
    val diff = this.getJdbcTemplate.queryForObject(sql, classOf[Integer], appId, appId);
    diff <= 0;
  }

  def getAttrCount(appId: Integer, eventId: Integer, owner: String): String = {
    //val sql="select count(*) from company_app a,event b,event_attr c where a.is_delete = 0 and b.is_delete = 0 and b.is_stop = 0 and a.id = b.app_id and b.id = c.event_id and c.is_stop = 0 b.id = ?;"
    val sql = "select max(cast(substring(c.column_name,4) as unsigned)) as cus from company_app a,event b,event_attr c where a.is_delete = 0 and b.is_delete = 0 and b.is_stop = 0 and a.id = b.app_id and b.id = c.event_id and c.is_stop = 0 AND  b.id = ?  AND a.id=?  AND b.owner=?"
    val maxCus = this.getJdbcTemplate.queryForObject(sql, classOf[String], eventId, appId, owner)
    maxCus
  }

  /**
    * 获取首次记录
    *
    * @param zg_eid
    */
  def getFrequency(zg_eid: Long, lid: Long) = {
    val sql = "select event_id from ads_frequency_first where event_id = ? and link_id = ?";
    val list = this.getJdbcTemplate.queryForList(sql, classOf[Integer], zg_eid.toString, lid.toString)
    if (list.size() == 0) {
      false
    } else {
      true
    }
  }

  /**
    * 获取首次记录 加入缓存
    */
  def getAdsFrequency(): mutable.Set[String] = {
    val sql = "select event_id,link_id,zg_id from ads_frequency_first ";
    val list = this.getJdbcTemplate.queryForList(sql)
    val result = new mutable.HashSet[String]()
    val iterator = list.iterator()
    while (iterator.hasNext) {
      val row = iterator.next()
      val eventId = Option(row.get("event_id")).map(_.toString.toInt.asInstanceOf[Integer]).orNull;
      val lid = Option(row.get("link_id")).map(_.toString.toInt.asInstanceOf[Integer]).orNull;
      val zgid = Option(row.get("zg_id")).map(_.toString.toInt.asInstanceOf[Integer]).orNull;
      result.add(s"${eventId}_${lid}_${zgid}");
    }
    return result
  }

  /**
    * 记录第一次的回传事件
    *
    * @param zg_eid
    * @return
    */
  def insertFrequency(eventId: String, linkId: Long, zgid: Integer, channelEvent: String, first_time: String) = {
    //ads_frequency_first 需要创建
    val sql = "insert into ads_frequency_first values(" + eventId + "," + linkId + "," + zgid + ",\'" + channelEvent + "\',\'" + first_time + "\')";
    //this.getJdbcTemplate.update(sql,String.valueOf(adsFrequencyFirst.event_id),String.valueOf(adsFrequencyFirst.link_id),String.valueOf(adsFrequencyFirst.channel_event),String.valueOf(adsFrequencyFirst.first_time))
    this.getJdbcTemplate.update(sql)

  }

  /**
    * 转化事件
    * 目前仅一个条件
    *
    * @param batchType
    * @return
    */
  def getConvent(event_id: Integer, lid: Integer): AdsLinkEvent = {
    val sql = "select link_id,event_id,channel_event,match_json,frequency from ads_link_event where event_id = ? and link_id = ?";
    // val labelRowMapper = BeanPropertyRowMapper.newInstance(classOf[AdsLinkEvent])

    val list = this.getJdbcTemplate.queryForList(sql, event_id, lid)
    if (list != null && list.size != 0) {
      val map = list.get(0)
      val adsLinkEvent = new AdsLinkEvent()
      adsLinkEvent.link_id = Option(map.get("link_id")).map(_.toString.toInt.asInstanceOf[Integer]).orNull
      adsLinkEvent.event_id = Option(map.get("event_id")).map(_.toString.toInt.asInstanceOf[Integer]).orNull
      adsLinkEvent.channel_event = map.get("channel_event").asInstanceOf[String]
      adsLinkEvent.match_json = map.get("match_json").asInstanceOf[String]
      adsLinkEvent.frequency = Option(map.get("frequency")).map(_.toString.toInt.asInstanceOf[Integer]).orNull
      return adsLinkEvent
      map //目前设计中，仅存在一个属性条件
    }
    null
  }

  /**
    * 获取广告深度回传事件条件
    */
  def getAdsLinkEventMap(): Map[String, AdsLinkEvent] = {
    val sql = "select link_id,event_id,event_ids,channel_event,match_json,frequency,windows_time from ads_link_event where  is_delete=0"
    val list = this.getJdbcTemplate.queryForList(sql)
    var mapResult = Map[String, AdsLinkEvent]()
    val iterator = list.iterator()
    while (iterator.hasNext) {
      val row = iterator.next()
      val linkId = Option(row.get("link_id")).map(_.toString.toInt.asInstanceOf[Integer]).orNull
      val eventId = Option(row.get("event_id")).map(_.toString.toInt.asInstanceOf[Integer]).orNull
      val channelEvent = row.get("channel_event").asInstanceOf[String]
      val matchJson = row.get("match_json").asInstanceOf[String]
      val frequency = Option(row.get("frequency")).map(_.toString.toInt.asInstanceOf[Integer]).orNull
      var windowTime = 2592000L
      if (null != row.get("windows_time")) {
        windowTime  = row.get("windows_time").toString.toLong
      }
      val eventIds = row.get("event_ids").asInstanceOf[String]

      val adsLinkEvent = new AdsLinkEvent()
      adsLinkEvent.link_id = linkId
      adsLinkEvent.event_id = eventId
      adsLinkEvent.event_ids = eventIds
      adsLinkEvent.channel_event = channelEvent
      adsLinkEvent.match_json = matchJson
      adsLinkEvent.frequency = frequency
      adsLinkEvent.window_time = windowTime
      val key = s"${eventId}_${linkId}"
      mapResult = mapResult.+(key -> adsLinkEvent);
    }
    mapResult
  }

  /**
    * 获取链接ID和渠道事件 channel_event 是event type
    *
    * @return
    */
  def getLidAndChannelEvent(): Map[String, String] = {
    val sql = "select link_id,event_id,channel_event from ads_link_event where is_delete = 0"
    val list = this.getJdbcTemplate.queryForList(sql)
    var mapResult = Map[String, String]()

    val iterator = list.iterator()
    while (iterator.hasNext) {
      val row = iterator.next()
      val lid = Option(row.get("link_id")).map(_.toString.toInt.asInstanceOf[Integer]).orNull;
      val eventId = Option(row.get("event_id")).map(_.toString.toInt.asInstanceOf[Integer]).orNull;
      val channelEvent = row.get("channel_event").asInstanceOf[String];
      val key = s"${lid}_${eventId}"
      mapResult = mapResult.+(key -> channelEvent);
    }
    mapResult
  }

  /**
    * 获取自定义参数
    *
    * @param event_id
    * @return
    */
  def getUTM(lid: Integer): Utm = {
    val sql = "select utm_campaign,utm_source,utm_medium,utm_term,utm_content from ads_link_toufang where id = ?";

    val list = this.getJdbcTemplate.queryForList(sql, lid + "")
    if (list != null && list.size != 0) {
      val map = list.get(0)
      val utm = new Utm()
      utm.utm_campaign = map.get("utm_campaign").asInstanceOf[String]
      utm.utm_source = map.get("utm_source").asInstanceOf[String]
      utm.utm_medium = map.get("utm_medium").asInstanceOf[String]
      utm.utm_term = map.get("utm_term").asInstanceOf[String]
      utm.utm_content = map.get("utm_content").asInstanceOf[String]
      return utm
    }
    null
  }

  /**
    * 转化事件列表
    */

  /**
    * 获取链接ID和渠道事件
    *
    * @return
    */
  def getEIdMap(): Map[Integer, Integer] = {
    val sql = "select link_id,event_id from ads_link_event where is_delete = 0"
    val list = this.getJdbcTemplate.queryForList(sql)
    var mapResult = Map[Integer, Integer]()

    val iterator = list.iterator()
    while (iterator.hasNext) {
      val row = iterator.next()
      val lid = Option(row.get("link_id")).map(_.toString.toInt.asInstanceOf[Integer]).orNull;
      val eventId = Option(row.get("event_id")).map(_.toString.toInt.asInstanceOf[Integer]).orNull;
      mapResult = mapResult.+(lid -> eventId);
    }
    mapResult
  }

  /**
    * 判断应用是否开启广告投放
    */
  def isAdApp(appKey: String): Boolean = {
    val sql = "select stop from advertising_app where is_delete = 0 and app_key = ? "
    val list = this.getJdbcTemplate.queryForList(sql, appKey);
    if (!list.isEmpty) {
      val stop = Option(list.get(0).get("stop")).map(_.toString.toInt.asInstanceOf[Integer]).orNull
      if (stop == 0) {
        return true
      }
    }
    false
  }

  def getOpenAdvertisingFunctionAppId(): Map[String, Integer] = {
    val sql = "select app_key,id from advertising_app where is_delete = 0 and stop = 0;"
    val list = this.getJdbcTemplate.queryForList(sql)
    var mapResult = Map[String, Integer]()
    val iterator = list.iterator()
    while (iterator.hasNext) {
      val row = iterator.next()
      val appKey = row.get("app_key").asInstanceOf[String];
      val appId = Option(row.get("id")).map(_.toString.toInt.asInstanceOf[Integer]).orNull;
      mapResult = mapResult.+(appKey -> appId);
    }
    mapResult
  }

  def putEventLastInsertTime(eventlastInsertTime: scala.collection.concurrent.Map[Int, String]): Unit = {
    var sqls = ArrayBuffer[String]()
    var count = 0
    eventlastInsertTime.foreach(x => {
      val sql = "UPDATE `event` SET `last_insert_time` = '" + x._2 + "' WHERE `id` = " + x._1
      sqls += sql
      count = count + 1
      if (count >= 100) {
        batchUpdate(sqls)
        count = 0
        sqls = ArrayBuffer[String]()
      }
    })
    if (sqls.nonEmpty) {
      batchUpdate(sqls)
    }
  }

  def batchUpdate(sqls: ArrayBuffer[String]): Unit = {
    this.getJdbcTemplate.batchUpdate(sqls.toArray)
  }

  /**
    * 获取虚拟事件信息
    */
  def getVirtualEventMap(): Map[String, java.util.ArrayList[String]] = {
    val sql = "select event_name,alias_name,app_id,event_json from virtual_event where is_delete=0 and event_status=0"
    val list = this.getJdbcTemplate.queryForList(sql)
    var mapResult = Map[String, java.util.ArrayList[String]]()

    val iterator = list.iterator()
    while (iterator.hasNext) {
      val row = iterator.next()
      val virtualEventName = row.get("event_name").asInstanceOf[String];
      val virtualEventAliasName = row.get("alias_name").asInstanceOf[String];
      val appId = Option(row.get("app_id")).map(_.toString.toLong).getOrElse(0L);
      val eventJson = row.get("event_json").asInstanceOf[String];
      val jsonObj: JSONObject = JSON.parseObject(eventJson)
      jsonObj.put("virtual_name", virtualEventName);
      jsonObj.put("virtual_alias", virtualEventAliasName);
      val owner = jsonObj.getString("owner")
      val eventName = jsonObj.getString("eventName")
      val key = s"${appId}_${owner}_${eventName}"
      val optionValue = mapResult.get(key)
      if (optionValue.isDefined) {
        val arrJson = optionValue.get
        arrJson.add(jsonObj.toJSONString);
        mapResult = mapResult.+(key -> arrJson);
      } else {
        val arrJson = new java.util.ArrayList[String]
        arrJson.add(jsonObj.toJSONString);
        mapResult = mapResult.+(key -> arrJson);
      }

    }
    mapResult
  }

  /**
    * 获取虚拟事件属性信息
    */
  def getVirtualEventAttrMap(): Map[String, Set[String]] = {
    val sql = "select event_name,app_id,event_json from virtual_event where is_delete=0 and event_status=0"
    val list = this.getJdbcTemplate.queryForList(sql)
    var mapResult = Map[String, Set[String]]()
    val iterator = list.iterator()
    while (iterator.hasNext) {
      val row = iterator.next()
      val virtualEventName = row.get("event_name").asInstanceOf[String];
      val appId = Option(row.get("app_id")).map(_.toString.toLong).getOrElse(0L);
      val eventJson = row.get("event_json").asInstanceOf[String];
      val jsonObj: JSONObject = JSON.parseObject(eventJson)
      val owner = jsonObj.getString("owner")
      val eventName = jsonObj.getString("eventName")
      val bindEventAttr = jsonObj.getJSONArray("attrs")
      if (bindEventAttr != null) {
        val key = s"${appId}_${virtualEventName}_${owner}_${eventName}"
        var valueOpt = mapResult.get(key)
        if (valueOpt.isEmpty) {
          valueOpt = Option.apply(Set[String]())
        }
        var set = valueOpt.get
        for (i <- 0 until bindEventAttr.size()) {
          val eventAttr = bindEventAttr.getString(i)
          set = set.+(eventAttr)
        }
        mapResult = mapResult.+(key -> set);
      }
    }
    mapResult
  }

  /**
    * 获取埋点事件属性别名
    */
  def getEventAttrAliasMap(): Map[String, String] = {
    val sql = "select t1.attr_name as attr_name,t1.alias_name as alias_name,t2.event_name as event_name, t2.app_id as app_id,t2.owner as owner from  (select event_id,attr_name,alias_name from event_attr where alias_name is not null and alias_name!='')t1 left join  (select id,event_name,app_id,owner from event where is_delete=0 )t2 on t1.event_id=t2.id where t2.id is not null "
    val list = this.getJdbcTemplate.queryForList(sql)
    var mapResult = Map[String, String]()

    val iterator = list.iterator()
    while (iterator.hasNext) {
      val row = iterator.next()
      val attrName = row.get("attr_name").asInstanceOf[String]
      val aliasName = row.get("alias_name").asInstanceOf[String]
      val eventName = row.get("event_name").asInstanceOf[String]
      val appId = row.get("app_id").asInstanceOf[Long]
      val owner = row.get("owner").asInstanceOf[String]

      var key = s"${appId}_${owner}_${eventName}_${attrName}"
      mapResult = mapResult.+(key -> aliasName);
    }
    mapResult
  }

  /**
    * 获取虚拟事件应用id
    */
  def getVirtualEventAppidsSet(): mutable.Set[String] = {
    val sql = "select app_id from virtual_event where is_delete=0 and event_status=0 group by app_id"
    val list = this.getJdbcTemplate.queryForList(sql)
    val result = new mutable.HashSet[String]()
    val iterator = list.iterator()
    while (iterator.hasNext) {
      val row = iterator.next()
      val appId = row.get("app_id").asInstanceOf[Long];
      result.add(appId.toString)
    }
    result
  }


}
