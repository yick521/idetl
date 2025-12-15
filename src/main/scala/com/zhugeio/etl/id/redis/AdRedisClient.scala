package com.zhugeio.etl.id.redis

import cn.hutool.core.util.{CharsetUtil, HexUtil}
import cn.hutool.crypto.SmUtil
import com.zhugeio.etl.id.Config
import com.zhugeio.etl.id.adtoufang.common.AdMessage
import org.apache.commons.lang.StringUtils
import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig, Response}

import java.util.{Date, Properties}
import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
  * Created by ziwudeng on 9/12/16.
  */
object AdRedisClient {

  val appPre = "adtfad"
  val dbPrefix = "ad_redis"
  var properties = getPro(Config.JEDIS_FILE)
  val jedisPool = get()

  def getPro(propertyFile: String): Properties = {
    val properties = new Properties()
    properties.load(this.getClass.getClassLoader.getResourceAsStream(propertyFile))
    properties
  }

  def get(): JedisPool = {
    val config = new JedisPoolConfig
    config.setTestOnBorrow(true);
    config.setTestOnReturn(true);
    config.setMaxTotal(Integer.parseInt(properties.getProperty(dbPrefix + ".pool.maxTotal")));
    config.setMaxIdle(Integer.parseInt(properties.getProperty(dbPrefix + ".pool.maxIdle")));
    config.setMinIdle(Integer.parseInt(properties.getProperty(dbPrefix + ".pool.minIdle")));
    config.setMaxWaitMillis(Integer.parseInt(properties.getProperty(dbPrefix + ".pool.maxWaitMillis")));
    if (properties.getProperty(dbPrefix + ".password").trim.nonEmpty) {
      var password = properties.getProperty(dbPrefix + ".password")
      val etype = Config.getProp(Config.ENCRYPTION_TYPE, "0").toInt
      if (etype == 2) {
        val sm4Key = Config.readFile(Config.getProp(Config.SM4_PRIKEY_PATH))
        val sm4 = SmUtil.sm4(HexUtil.decodeHex(sm4Key))
        password = sm4.decryptStr(password, CharsetUtil.CHARSET_UTF_8)
      }
      new JedisPool(config, properties.getProperty(dbPrefix + ".host"), Integer.parseInt(properties.getProperty(dbPrefix + ".port")), Integer.parseInt(properties.getProperty(dbPrefix + ".timeout")), password)
    } else {
      new JedisPool(config, properties.getProperty(dbPrefix + ".host"), Integer.parseInt(properties.getProperty(dbPrefix + ".port")), Integer.parseInt(properties.getProperty(dbPrefix + ".timeout")))
    }
  }


  def doInJedis(callback: Jedis => AnyRef): Unit = {
    var redisConnection: Jedis = null
    try {
      redisConnection = jedisPool.getResource
      callback(redisConnection)
    } finally {
      if (redisConnection != null) {
        redisConnection.close()
      }
    }
  }


  def test(): Unit = {
    doInJedis(connection => {
      connection.set("a", "3")
    })
  }

  /**
    * 批量获取string类型数据，  part1:part2:${part3}
    * 返回
    */
  def mgetResult(params: collection.mutable.HashSet[String], isNull: Boolean): mutable.HashMap[String, String] = {
    if (params.size == 0) {
      return new collection.mutable.HashMap[String, String]
    }

    val arrays = params.toArray
    var result: java.util.List[String] = null
    this.doInJedis(jedis => {
      result = jedis.mget(arrays: _*)
      null
    })
    val iterator = result.iterator()
    val returnResult = new mutable.HashMap[String, String]()
    arrays.foreach(key => {
      val itemResult = iterator.next()
      if (StringUtils.isNotBlank(itemResult)) {
        returnResult.put(key, itemResult)
      } else {
        if (isNull) {
          returnResult.put(key, null)
        }
      }
    })
    returnResult
  }

  /**
    * 将 string 类型的 kv 存入rssdb
    */
  def setStr(params: mutable.HashMap[String, String], time: Integer): Unit = {
    if (params.isEmpty) {

    } else {
      this.doInJedis(jedis => {
        val pipline = jedis.pipelined()
        params.foreach(m => {
          pipline.set(m._1, m._2)
          pipline.expire(m._1, time)
        })
        pipline.sync()
        null
      })
    }

  }

  /**
    * 获取ssdb配置中的值
    */

  def getProValue(proName: String): String = {
    properties.getProperty(dbPrefix + "." + proName)
  }

  /**
    * get =>（key，value）
    */
  def getStr(key: String): String = {
    if (key.size == 0) {
      return ""
    }
    var result: String = ""
    this.doInJedis(jedis => {
      result = jedis.get(key)
      null
    })
    result
  }

  /**
    * set =>（key，value）
    */
  def setStr(key: String, value: String, expireTime: Integer): Unit = {
    this.doInJedis(jedis => {
      jedis.set(key, value)
      jedis.expire(key, expireTime) //过期时间设为24小时
    })
  }
}
