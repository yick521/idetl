package com.zhugeio.etl.id

import java.io.{BufferedReader, FileInputStream, InputStreamReader}
import java.nio.charset.StandardCharsets
import java.util.Properties

/**
  * Created by ziwudeng on 9/7/16.
  */
object Config {
  val OPEN_TOUFANG_LOG = "open_toufang_log"

  val OPEN_TOUFANG = "open_toufang"

  val IMPALA_DB_FILE = "impala_db.properties";

  val FRONT_DB_FILE = "front_db.properties";

  val DW_DB_FILE = "dw_db.properties";

  val JEDIS_FILE = "id_redis.properties";

  val DTYPE_TOPIC_JSON = "dtype_topic.json"

  val KAFKA_BROKERS = "kafka.brokers"

  val KAFKA_SOURCE_TOPIC = "kafka.sourceTopic"

  val KAFKA_GROUP_ID = "kafka.group.id"

  val KAFKA_BUFFER_MEMORY = "kafka.buffer.memory"

  val KAFKA_PRODUCER_NUMBER = "kafka.producer.singles";

  val MAX_PROP_LENGTH = "max_prop_length"

  val MAX_EVENT_LENGTH = "max_event_length"

  val MAX_ATTR_LENGTH = "max_attr_length"

  val MAX_DEV_PROP_LENGTH = "max_dev_prop_length"

  val ID_TIMEOUT_SECONDS = "id_timeout_seconds"

  val SDK_CONFIG_FLUSH = "sdk_config_flush"

  val COMPANY_IDENTIFILER = "company.identifier"

  val PUBLIC_KEY = "public.key"

  val LICENSE = "license"

  val SM4_PRIKEY_PATH = "sm4_priKey_path"

  val ENCRYPTION_TYPE = "encryption_type"

  val CAFFINE_CACHE_EXPIRE_MINUTES = "caffine_cache_expire_minutes"
  val CAFFINE_CACHE_MAX_SIZE = "caffine_cache_max_size"

  val properties = new Properties()

  properties.load(new InputStreamReader(this.getClass.getClassLoader.getResourceAsStream("config.properties"), StandardCharsets.UTF_8));

  def getProp(key: String): String = {
    properties.getProperty(key);
  }

  def getProp(key: String, defaultValue: String): String = {
    properties.getProperty(key, defaultValue)
  }

  def getInt(key: String): Int = {
    properties.getProperty(key).toInt
  }

  def getInt(key: String,defaultValue:Int): Int = {
    if(getProp(key) != null) {
      getProp(key).toInt
    }else{
      defaultValue
    }
  }

  def getLong(key: String, defaultValue: Long): Long = {
    if(getProp(key) != null) {
      getProp(key).toLong
    }else{
      defaultValue
    }
  }

  def getBoolean(key: String, defaultValue: Boolean): Boolean = {
    if(getProp(key) != null) {
      getProp(key).toBoolean
    }else{
      defaultValue
    }
  }

  def readFile(filename: String): String = {
    val ir = new BufferedReader(new InputStreamReader(new FileInputStream(filename)))
    ir.readLine()
  }
}
