//package com.zhugeio.etl.id
//
//import com.zhugeio.etl.common.kafka.OffsetHandler
//import com.zhugeio.etl.id.service.MainService
//import kafka.common.TopicAndPartition
//import kafka.message.MessageAndMetadata
//import kafka.serializer.StringDecoder
//import org.apache.spark.SparkConf
//import org.apache.spark.streaming._
//import org.apache.spark.streaming.kafka._
//
//import scala.collection.mutable.ListBuffer
//
//object TmpPayIdApp {
//  def main(args: Array[String]) {
//
//    val Array(brokers, topics, groupId) = Array(Config.getProp(Config.KAFKA_BROKERS), Config.getProp(Config.KAFKA_SOURCE_TOPIC) , Config.getProp(Config.KAFKA_GROUP_ID))
//
//    val sparkConf = new SparkConf().setAppName("IdApp")
//    //    sparkConf.setMaster("local[3]")
//    val ssc = new StreamingContext(sparkConf, Seconds(args(0).toLong))
//
//    val topicsSet = topics.split(",").toSet
//    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers,"fetch.message.max.bytes"->"10000121")
//    // offset handler
//    val offsetHandler = new OffsetHandler(kafkaParams)
//
//    //get offset and define messageHandler
//    val offsetsResult = offsetHandler.getOffsets(groupId, topicsSet)
//    var offset: Map[TopicAndPartition, Long] = null
//    if (offsetsResult.isLeft) {
//      throw new RuntimeException("get offset Fail:" + offsetsResult.left.get.toString())
//    } else {
//      offset = offsetsResult.right.get
//    }
//
//    println("global begin offset :"+ groupId +"," + offset.mkString(","))
//
//    val messageHandler = (mmd: MessageAndMetadata[String, String]) => (mmd.key, mmd.message,mmd.partition)
//
//    //get stream
//    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, (String, String,Int)](
//      ssc, kafkaParams, offset, messageHandler)
//
//
//    messages.foreachRDD { rdd =>
//
//      println("begin offset :" + rdd.asInstanceOf[HasOffsetRanges].offsetRanges.mkString(","))
//      val result = rdd.foreachPartition { partitionOfRecords => {
//        var msgs = new ListBuffer[ZGMessage]()
//        var partition = 0
//        partitionOfRecords.foreach(msg => {
//          msgs += new ZGMessage("1", msg._3, 1, msg._1, msg._2)
//          partition = msg._3
//        })
//
//        if (msgs.nonEmpty) {
//          MainService.process(msgs,false)
//        }
//      }
//      }
//
//      println(result)
//
//      //save offset
//      val saveResult = offsetHandler.saveOffsets(groupId, Map(rdd.asInstanceOf[HasOffsetRanges].offsetRanges.map(o => {
//        o.topicAndPartition() -> o.untilOffset
//      }): _*))
//
//      if (saveResult.isLeft) {
//        println("save offset fail:" + rdd.asInstanceOf[HasOffsetRanges].offsetRanges.mkString(","))
//        throw new RuntimeException("save offset Fail:" + saveResult.left.get.toString() + ",offsets:" + rdd.asInstanceOf[HasOffsetRanges].offsetRanges.mkString(","))
//      } else {
//        println("save offset success:" + rdd.asInstanceOf[HasOffsetRanges].offsetRanges.mkString(","))
//      }
//
//    }
//
//    ssc.start()
//    ssc.awaitTermination()
//  }
//
//}