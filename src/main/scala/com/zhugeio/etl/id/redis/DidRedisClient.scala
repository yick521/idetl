package com.zhugeio.etl.id.redis

import cn.hutool.core.util.{CharsetUtil, HexUtil}
import cn.hutool.crypto.SmUtil

import java.util.Properties
import com.zhugeio.etl.id.Config
import com.zhugeio.etl.id.commons.CaffeineCache
import com.zhugeio.etl.id.log.IdLogger
import org.apache.commons.lang3.StringUtils
import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig, Response}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
  * Created by ziwudeng on 9/12/16.
  */
object DidRedisClient {


  val jedisPool = get(Config.JEDIS_FILE, "did_redis")

  def get(propertyFile: String, dbPrefix: String): JedisPool = {
    val properties = new Properties()
    properties.load(this.getClass.getClassLoader.getResourceAsStream(propertyFile))
    val config = new JedisPoolConfig
    config.setTestOnBorrow(true);
    config.setTestOnReturn(false);
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
    if (params.isEmpty) {
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

  def getMap(params: mutable.HashSet[String],mapResultGet:collection.mutable.HashMap[String,String]): mutable.HashSet[String] = {
    val notInSSDBSet:mutable.HashSet[String] =  mutable.HashSet[String]()
    if (params==null) {
      IdLogger.error(s"getMap_params_is_null,mapResultGet:${mapResultGet}",new NullPointerException())
      return notInSSDBSet
    }
    if (params.isEmpty) {
      return notInSSDBSet
    }
    //get map
    val mapParam = new mutable.HashMap[String, ListBuffer[String]]()
//    val time0= System.currentTimeMillis()
    params.foreach(s => {
      val split = s.split(":", 3)
      val key = split(0) + ":" + split(1)
      val hkey = split(2)
      if (!mapParam.contains(key)) {
        mapParam.put(key, new ListBuffer[String]);
      }
      mapParam(key) += hkey
    })
    val time1= System.currentTimeMillis()
//      IdLogger.info(s"getMap_get_mapParam,time:"+(time1-time0)+",params="+params.size)


    val mapFirst = new mutable.HashMap[String, Response[java.util.List[String]]]()
    this.doInJedis(jedis => {
      val pipline = jedis.pipelined()
      mapParam.foreach(m => {
        mapFirst.put(m._1, pipline.hmget(m._1, m._2.toArray: _*))
      })
      pipline.sync()
      null
    })
    val time2= System.currentTimeMillis()
      IdLogger.info(s"handleDeviceids_getfromSsdb_query,time:"+(time2-time1)+",mapParam="+mapParam.size)



    mapParam.foreach(m => {
      val iterator = mapFirst(m._1).get().iterator()
      m._2.foreach(s => {
        val result = iterator.next()
        mapResultGet.put(m._1+":"+s,result)
        if(StringUtils.isNotBlank(result)){
          CaffeineCache.putDeviceCache(m._1+":"+s,result)
//          println("getDeviceMapFromSSD:"+m._1+":"+s+":"+result)
        }else{
          notInSSDBSet.add(m._1+":"+s)
        }
      })
    })
//    val time3= System.currentTimeMillis()
//    IdLogger.info(s"getMap_get_notInSSDBMap,time:"+(time3-time2)+",mapFirst="+mapFirst.size)
    notInSSDBSet
  }
}
