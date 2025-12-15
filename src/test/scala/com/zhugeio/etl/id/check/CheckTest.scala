package com.zhugeio.etl.id.check

import com.alibaba.fastjson.{JSON, JSONObject}
import com.zhugeio.etl.id.adtoufang.common.{AdMessage, ConvertMessage}
import com.zhugeio.etl.id.dao.FrontDao
import com.zhugeio.etl.id.redis.{AdRedisClient, DidRedisClient, RedisClient, UidRedisClient}

import java.io.{BufferedReader, InputStreamReader}
import com.zhugeio.etl.id.{Config, ZGMessage}
import org.junit._
import redis.clients.jedis.{Jedis, Response}

import java.util
import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
  * Created by ziwudeng on 11/21/16.
  */
class CheckTest {

  @Test
  def testBasic(): Unit = {
    var msgs = new ListBuffer[ZGMessage]()
    val ir = new BufferedReader(new InputStreamReader(this.getClass.getClassLoader.getResourceAsStream("sample_web.txt")))
    var line: String = null
    val iterator = ir.lines().iterator()
    while (iterator.hasNext) {
      val result = Check.checkBasic(iterator.next())
      if (result.isRight) {
        println("right")
      } else {
        println("wrong")
        result.left.get.foreach(m => {
          println(m)
        })
      }
    }
  }

  @Test
  def testRedisHmget(): Unit = {
    var jedisPool = DidRedisClient.get(Config.JEDIS_FILE, "uid_redis")
    val jedis: Jedis = jedisPool.getResource
    val pipeline = jedis.pipelined()
    //    jedis.set("test", "value");
    //    jedis.expire("test", 100000);
    //    println(jedis.get("test"))
    //    println(jedis.ttl("test"))
    pipeline.hset("lid:3", "1", "1123")
    pipeline.hset("lid:3", "2", "1124")
    pipeline.expire("lid:3", 100000);
    pipeline.sync()
    var lidIds = mutable.HashSet[String]()
    var lidIdMap = mutable.HashMap[String, String]()
    //lidIds += "lid:" + appId + ":" + zgid;
    lidIds += "lid:3:1";
    lidIds += "lid:3:2";
    println(jedis.hget("lid:3", "1"))
    lidIdMap = RedisClient.getMap(lidIds, true)
    println(lidIdMap)
    //    println(lidIdMap)
    //    val pipline = jedis.pipelined()
    //    var sets = mutable.HashSet[String]()
    //    sets+="1"
    //    sets+="2"
    //    val value: Response[util.List[String]] = pipline.hmget("lid:3", sets.toArray: _*)
    //    pipline.sync()
    //    println(value.get())


  }

  @Test
  def testRedismget(): Unit = {
    var jedisPool = DidRedisClient.get(Config.JEDIS_FILE, "uid_redis")
    val jedis: Jedis = jedisPool.getResource
    //    jedis.set("test", "value");
    //    jedis.expire("test", 100000);
    //    println(jedis.get("test"))
    //    println(jedis.ttl("test"))
    jedis.set("lid:3:1", "1123")
    jedis.set("lid:3:2", "1124")
    jedis.expire("lid:3:1", 100000);
    jedis.expire("lid:3:2", 100000);
    var lidIds = mutable.HashSet[String]()
    //lidIds += "lid:" + appId + ":" + zgid;
    lidIds += "lid:3:1";
    lidIds += "lid:3:2";
    //    val pipline = jedis.pipelined()
    //    val value: Response[util.List[String]] = pipline.mget( lidIds.toArray: _*)
    //    pipline.sync()
    //println(value.get())
    println(AdRedisClient.mgetResult(lidIds,true))
  }

  @Test
  def testRedismset(): Unit = {
    var jedisPool = DidRedisClient.get(Config.JEDIS_FILE, "uid_redis")
    val jedis: Jedis = jedisPool.getResource
    //    jedis.set("test", "value");
    //    jedis.expire("test", 100000);
    //    println(jedis.get("test"))
    //    println(jedis.ttl("test"))
    jedis.set("lid:3:1", "1123")
    jedis.set("lid:3:2", "1124")
    jedis.expire("lid:3:1", 100000);
    jedis.expire("lid:3:2", 100000);
    var lidIds = mutable.HashSet[String]()
    //lidIds += "lid:" + appId + ":" + zgid;
    lidIds += "lid:3:1";
    lidIds += "lid:3:2";
    //    val pipline = jedis.pipelined()
    //    val value: Response[util.List[String]] = pipline.mset(lidIds.toArray: _*)
    //    pipline.sync()
    //println(value.get())
    println(AdRedisClient.mgetResult(lidIds,true))
  }

  @Test
  def testMysqlCon(): Unit = {
    //配置文件front_db.property 中的配置 front_db.url  有对应的库名为 sdkv
    val sql = "select id from company_app where app_key = ? and is_delete = 0 and stop = 0 and id not in (select id from tmp_transfer where status = 2)"
    val list: util.List[util.Map[String, AnyRef]] = FrontDao.getJdbcTemplate.queryForList(sql, "ce250a2bcdee4fdaa0838600fafb9769")
    list.get(0).get("id")
    println(list.get(0).get("id"))
  }

  @Test
  def testJson(): Unit = {
    val jsonObj = new JSONObject()
    val value = new ConvertMessage()
    jsonObj.put("tableName", "toufang_convert_event")
    jsonObj.put("sinkType", "kudu")
    jsonObj.put("data", value.toConvertJson())
    println(jsonObj.toJSONString.replace("\\\"","\"").replace("\"{","{").replace("}\"","}"))
  }
  @Test
  def testRedisDel(): Unit = {
    var jedisPool = DidRedisClient.get(Config.JEDIS_FILE, "uid_redis")
    val jedis: Jedis = jedisPool.getResource
    val pipeline = jedis.pipelined()
    var count=0
    while(count<10){
      pipeline.set("key"+count,"value"+count)
      pipeline.expire("key"+count,700)
      count=count+1
    }
    pipeline.sync()
    count=0
    val jsonObj = new JSONObject()
    while(count<10){
      jsonObj.put("key" + count,jedis.get("key" + count))
      count=count+1
    }
    println(jsonObj)
//    println("删除前"+jedis.mget(list: _*))
//    println(jedis.del(thisDelKeySet.toArray: _*))
//    println("删除后"+jedis.mget(list: _*))

    jedis.close()
    //jedis.set("key1","value1")
    //jedis.expire("key1",700)
    //println(jedis.get("key1"))

    //   jedis.del(delKey)
  }
}
