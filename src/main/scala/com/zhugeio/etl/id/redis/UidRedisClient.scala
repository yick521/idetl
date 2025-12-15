package com.zhugeio.etl.id.redis

import cn.hutool.core.util.{CharsetUtil, HexUtil}
import cn.hutool.crypto.SmUtil

import java.util.{Date, Properties}
import com.zhugeio.etl.id.Config
import org.apache.commons.lang.StringUtils
import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig, Response}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
  * Created by ziwudeng on 9/12/16.
  */
object UidRedisClient {

  val appPre = "adtfad"
  val dbPrefix = "uid_redis"
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

  def getMaxMap(params: collection.mutable.HashSet[String]): mutable.HashMap[String, java.lang.Long] = {
    if (params.size == 0) {
      return new collection.mutable.HashMap[String, java.lang.Long]
    }

    val arrays = params.toArray
    var result: java.util.List[String] = null
    this.doInJedis(jedis => {
      result = jedis.mget(arrays: _*)
      null
    })
    val iterator = result.iterator()
    val returnResult = new mutable.HashMap[String, java.lang.Long]()
    arrays.foreach(key => {
      val itemResult = iterator.next()
      val split = key.split(":")
      val appId = split(2)

      if (StringUtils.isNotBlank(itemResult)) {
        returnResult.put(appId, java.lang.Long.parseLong(itemResult))
      } else {
        returnResult.put(appId, 0L)
      }
    })
    returnResult
  }

  def getMap(params: mutable.HashSet[String], nullable: Boolean): mutable.HashMap[String, String] = {

    if (params.size == 0) {
      return new collection.mutable.HashMap[String, String]
    }
    //get map
    val mapParam = new mutable.HashMap[String, ListBuffer[String]]()

    params.foreach(s => {
      val split = s.split(":", 3)
      val key = split(0) + ":" + split(1)
      val hkey = split(2)
      if (!mapParam.contains(key)) {
        mapParam.put(key, new ListBuffer[String]);
      }
      mapParam.get(key).get += hkey
    })

    val mapFirst = new mutable.HashMap[String, Response[java.util.List[String]]]()
    this.doInJedis(jedis => {
      val pipline = jedis.pipelined()
      mapParam.foreach(m => {
        mapFirst.put(m._1, pipline.hmget(m._1, m._2.toArray: _*))
      })
      pipline.sync()
      null
    })

    val mapResultGet = new collection.mutable.HashMap[String, String]

    mapParam.foreach(m => {
      val iterator = mapFirst.get(m._1).get.get().iterator()
      m._2.foreach(s => {
        val result = iterator.next()
        if (nullable) {
          mapResultGet.put(m._1 + ":" + s, result)
        } else {
          if (StringUtils.isNotBlank(result)) {
            mapResultGet.put(m._1 + ":" + s, result)
          }
        }
      })
    })
    mapResultGet
  }
}
