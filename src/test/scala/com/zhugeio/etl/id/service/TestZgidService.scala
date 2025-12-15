package com.zhugeio.etl.id.service

import org.junit.Test

import scala.collection.mutable

/**
  * Created by Administrator on 2017/5/24.
  */
class TestZgidService {


  @Test
  def testZgid(): Unit = {
    val zgDidZgidMap = new mutable.HashMap[String, String]()

    zgDidZgidMap.put("uz:222:1", "1")

    val zgUidZgidMap = new mutable.HashMap[String, String]()
    zgUidZgidMap.put("dz:222:2", "2")

    val zgIds = mutable.HashSet() ++ zgDidZgidMap.map(r => {
      val split = r._1.split(":")
      val appId = split(1)
      "zu:" + appId + ":" + r._2
    }) ++ zgUidZgidMap.map(r => {
      val split = r._1.split(":")
      val appId = split(1)
      "zu:" + appId + ":" + r._2
    })
    println(zgIds)
  }


}
