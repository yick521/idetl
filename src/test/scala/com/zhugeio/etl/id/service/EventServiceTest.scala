package com.zhugeio.etl.id.service

import org.junit.Test

class EventServiceTest {


  @Test
  def maxEventLength(): Unit = {
    val s = "事件名称过长，大于128事件名称过长，大于128事件名称过长，大于128事件名称过长，大于128事件名称过长，大于128事件名\n称过长，大于128事件名hhh大于128事件名hhh大于128事件名hhh大于128事件名hhh大于128事hh大于128事hh大"
    if (s.length > 128) {
      println("err : ", s.length)
    } else {
      println("ok  : ", s.length)
    }

  }

}
