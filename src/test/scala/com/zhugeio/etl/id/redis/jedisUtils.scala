package com.zhugeio.etl.id.redis

import com.zhugeio.etl.id.Config
import com.zhugeio.tool.redis.RedisHandlerFactory
import org.junit.Test

class jedisUtils {

  @Test
  def createHabdler(): Unit = {
    val jedisPool = RedisHandlerFactory.createFtpHandler("jedis_pool")
    jedisPool.createRedisPool(Config.JEDIS_FILE, "id_redis")
    for (i <- 0 until 100) {
      jedisPool.set(s"test001-->${i}", i.toString)
      println(jedisPool.get(s"test001-->${i}"))
    }
  }


}
