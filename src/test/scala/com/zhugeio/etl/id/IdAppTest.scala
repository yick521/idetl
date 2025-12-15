package com.zhugeio.etl.id

import com.zhugeio.etl.id.service.MainService
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.{CanCommitOffsets, ConsumerStrategies, HasOffsetRanges, KafkaUtils}
import org.junit._

import scala.collection.mutable.ListBuffer

/**
  * Created by ziwudeng on 9/20/16.
  */
object IdAppTest {

  def main(args: Array[String]) {
    val logger = Logger.getLogger(this.getClass)
    val Array(brokers, topics, groupId) = Array(Config.getProp(Config.KAFKA_BROKERS), Config.getProp(Config.KAFKA_SOURCE_TOPIC), Config.getProp(Config.KAFKA_GROUP_ID))

    val sparkConf = new SparkConf()
      .set("spark.driver.host", "127.0.0.1")
      .setAppName("AdvCountApp")
    sparkConf.setMaster("local[*]")
    logger.info("RealTimeViewApp SparkConf初始成功")
    var seconds = 1L;
    if (args.size == 1) {
      seconds = args(0).toLong
    }
    val ssc = new StreamingContext(sparkConf, Seconds(seconds))

    val topicsSet = topics.split(",").toSet
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> brokers,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "key.serializer" -> "org.apache.kafka.common.serialization.StringSerializer",
      "value.serializer" -> "org.apache.kafka.common.serialization.StringSerializer",
      "auto.offset.reset" -> "earliest",
      "group.id" -> groupId,
      "enable.auto.commit" -> (false: java.lang.Boolean),
      "fetch.message.max.bytes" -> "10000121")

    val messages = KafkaUtils.createDirectStream[String, String](ssc, PreferConsistent, ConsumerStrategies.Subscribe[String, String](topicsSet, kafkaParams))
    messages.foreachRDD { rdd =>
      println("begin offset :" + rdd.asInstanceOf[HasOffsetRanges].offsetRanges.mkString(","))
      val result = rdd.foreachPartition {
        partitionOfRecords => {
          var msgs = new ListBuffer[ZGMessage]()
          partitionOfRecords.foreach(msg => {
            msgs += new ZGMessage(msg.topic(), msg.partition(), msg.offset(), msg.key(), msg.value())
          })
          if (msgs.nonEmpty) {
            MainService.process(msgs)
          }
        }
      }
      //save offset
      println(result)
      val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      messages.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)
    }
    ssc.start()
    ssc.awaitTermination()
  }

}
